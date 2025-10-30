/*
Copyright 2024 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package datadependency

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"syscall"
)

// ErrorType represents the category of error for better classification
type ErrorType string

const (
	ErrorTypeClient        ErrorType = "client"        // 4xx errors
	ErrorTypeServer        ErrorType = "server"        // 5xx errors
	ErrorTypeAuthentication ErrorType = "authentication" // Auth related errors
	ErrorTypeNotFound      ErrorType = "not_found"     // Resource not found
	ErrorTypeRateLimit     ErrorType = "rate_limit"    // Rate limiting
	ErrorTypeNetwork       ErrorType = "network"       // Network related
)

// HTTPError represents an HTTP error with enhanced metadata.
type HTTPError struct {
	Code      int                    `json:"code"`
	Message   string                 `json:"message"`
	Type      ErrorType              `json:"type,omitempty"`
	Details   map[string]interface{} `json:"details,omitempty"`
	Cause     error                  `json:"-"` // Original error that caused this HTTP error
}

func (e *HTTPError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("HTTP %d (%s): %s (caused by: %v)", e.Code, e.Type, e.Message, e.Cause)
	}
	return fmt.Sprintf("HTTP %d (%s): %s", e.Code, e.Type, e.Message)
}

func (e *HTTPError) StatusCode() int {
	return e.Code
}

// Unwrap returns the underlying cause error for error chain support
func (e *HTTPError) Unwrap() error {
	return e.Cause
}

// WithCause adds a cause error to the HTTP error
func (e *HTTPError) WithCause(cause error) *HTTPError {
	e.Cause = cause
	return e
}

// WithDetail adds a detail field to the HTTP error
func (e *HTTPError) WithDetail(key string, value interface{}) *HTTPError {
	if e.Details == nil {
		e.Details = make(map[string]interface{})
	}
	e.Details[key] = value
	return e
}

// NewHTTPError creates a new HTTP error with the given status code and message.
func NewHTTPError(code int, message string) *HTTPError {
	errorType := classifyErrorType(code)
	return &HTTPError{
		Code:    code,
		Message: message,
		Type:    errorType,
	}
}

// NewNotFoundError creates a standard HTTP 404 error.
func NewNotFoundError(message string) *HTTPError {
	return &HTTPError{
		Code:    http.StatusNotFound,
		Message: message,
		Type:    ErrorTypeNotFound,
	}
}

// NewInternalServerError creates a standard HTTP 500 error.
func NewInternalServerError(message string) *HTTPError {
	return &HTTPError{
		Code:    http.StatusInternalServerError,
		Message: message,
		Type:    ErrorTypeServer,
	}
}

// NewServiceUnavailableError creates a standard HTTP 503 error.
func NewServiceUnavailableError(message string) *HTTPError {
	return &HTTPError{
		Code:    http.StatusServiceUnavailable,
		Message: message,
		Type:    ErrorTypeServer,
	}
}

// NewTooManyRequestsError creates a standard HTTP 429 error.
func NewTooManyRequestsError(message string) *HTTPError {
	return &HTTPError{
		Code:    http.StatusTooManyRequests,
		Message: message,
		Type:    ErrorTypeRateLimit,
	}
}

// NewAuthenticationTimeoutError creates a standard HTTP 419 error (Iceberg specific).
func NewAuthenticationTimeoutError(message string) *HTTPError {
	return &HTTPError{
		Code:    419, // Authentication Timeout
		Message: message,
		Type:    ErrorTypeAuthentication,
	}
}

// NewBadRequestError creates a standard HTTP 400 error.
func NewBadRequestError(message string) *HTTPError {
	return &HTTPError{
		Code:    http.StatusBadRequest,
		Message: message,
		Type:    ErrorTypeClient,
	}
}

// NewUnauthorizedError creates a standard HTTP 401 error.
func NewUnauthorizedError(message string) *HTTPError {
	return &HTTPError{
		Code:    http.StatusUnauthorized,
		Message: message,
		Type:    ErrorTypeAuthentication,
	}
}

// NewForbiddenError creates a standard HTTP 403 error.
func NewForbiddenError(message string) *HTTPError {
	return &HTTPError{
		Code:    http.StatusForbidden,
		Message: message,
		Type:    ErrorTypeAuthentication,
	}
}

// NewBadGatewayError creates a standard HTTP 502 error.
func NewBadGatewayError(message string) *HTTPError {
	return &HTTPError{
		Code:    http.StatusBadGateway,
		Message: message,
		Type:    ErrorTypeServer,
	}
}

// NewGatewayTimeoutError creates a standard HTTP 504 error.
func NewGatewayTimeoutError(message string) *HTTPError {
	return &HTTPError{
		Code:    http.StatusGatewayTimeout,
		Message: message,
		Type:    ErrorTypeServer,
	}
}

// classifyErrorType automatically determines the error type based on HTTP status code
func classifyErrorType(code int) ErrorType {
	switch {
	case code == http.StatusNotFound:
		return ErrorTypeNotFound
	case code == http.StatusTooManyRequests:
		return ErrorTypeRateLimit
	case code == http.StatusUnauthorized || code == http.StatusForbidden || code == 419:
		return ErrorTypeAuthentication
	case code >= 400 && code < 500:
		return ErrorTypeClient
	case code >= 500 && code < 600:
		return ErrorTypeServer
	default:
		return ErrorTypeServer // Default fallback
	}
}

// ErrDataSourceNotFound is a sentinel error for plugins to wrap when a data source
// is not found. Plugins should use this or return HTTPError with 404 status.
var ErrDataSourceNotFound = NewNotFoundError("data source not found")

// statusCoder interface for errors that can provide HTTP status codes
type statusCoder interface {
	StatusCode() int
}

// IsNotFoundErr checks whether the error indicates a not found condition.
// It supports:
// 1. Sentinel error ErrDataSourceNotFound
// 2. Errors implementing statusCoder interface with 404 status
// 3. HTTPError with 404 status
func IsNotFoundErr(err error) bool {
	if err == nil {
		return false
	}

	// Check sentinel error
	if errors.Is(err, ErrDataSourceNotFound) {
		return true
	}

	// Check statusCoder interface
	if sc, ok := err.(statusCoder); ok {
		return sc.StatusCode() == http.StatusNotFound
	}

	// Check HTTPError type
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode() == http.StatusNotFound
	}

	return false
}

// IsRetryableHTTPError checks if an HTTP error is retryable based on Apache Iceberg REST API specification.
func IsRetryableHTTPError(err error) bool {
	if httpErr, ok := err.(*HTTPError); ok {
		switch httpErr.StatusCode() {
		case http.StatusTooManyRequests: // 429 - Too Many Requests
			return true
		case 419: // 419 - Authentication Timeout (Iceberg specific)
			return true
		case http.StatusServiceUnavailable: // 503 - Service Unavailable
			return true
		case http.StatusInternalServerError, // 500 - Internal Server Error
			http.StatusBadGateway,          // 502 - Bad Gateway
			http.StatusGatewayTimeout:      // 504 - Gateway Timeout
			return true
		}
		// 5xx errors in general are retryable
		if httpErr.StatusCode() >= 500 && httpErr.StatusCode() < 600 {
			return true
		}
	}
	return false
}

// IsNetworkError checks if an error is a network-related error that should be retried.
// This function provides a centralized way to identify network errors across all plugins.
func IsNetworkError(err error) bool {
	// Check for network operation errors
	if netErr, ok := err.(net.Error); ok {
		// Timeout errors are retryable
		if netErr.Timeout() {
			return true
		}
		// Temporary errors are retryable
		if netErr.Temporary() {
			return true
		}
	}

	// Check for specific network errors
	if opErr, ok := err.(*net.OpError); ok {
		// Connection refused, network unreachable, etc.
		if opErr.Op == "dial" || opErr.Op == "read" || opErr.Op == "write" {
			if sysErr, ok := opErr.Err.(*syscall.Errno); ok {
				switch *sysErr {
				case syscall.ECONNREFUSED, syscall.ENETUNREACH, syscall.EHOSTUNREACH:
					return true
				}
			}
		}
	}

	// Check for DNS errors
	if _, ok := err.(*net.DNSError); ok {
		return true
	}

	return false
}

// IsRetryableError provides a comprehensive check for retryable errors.
// It combines HTTP error checking and network error checking.
func IsRetryableError(err error) bool {
	return IsRetryableHTTPError(err) || IsNetworkError(err)
}