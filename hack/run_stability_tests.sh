#!/bin/bash

# Stability Test Script for Go Projects
# This script runs tests multiple times and logs the results for stability analysis

set -e

# Configuration
TEST_RUNS=${1:-50}        # Arg 1: Run count (Default: 50)
TEST_DIR=${2:-$(pwd)}     # Arg 2: Directory (Default: current directory)
TEST_PATTERN=${3:-""}     # Arg 3: Specific test pattern (Default: run all)

# Generate log filename based on pattern to avoid overwriting general logs
if [ -n "$TEST_PATTERN" ]; then
    # Sanitize pattern for filename (replace spaces/special chars with underscores)
    SAFE_PATTERN=$(echo "$TEST_PATTERN" | sed 's/[^a-zA-Z0-9]/_/g')
    LOG_FILE="$TEST_DIR/stability_test_${SAFE_PATTERN}_${TEST_RUNS}_runs.log"
else
    LOG_FILE="$TEST_DIR/stability_test_ALL_${TEST_RUNS}_runs.log"
fi

SUMMARY_FILE="$TEST_DIR/test_summary.log"

# Print Header
echo "=== Go Project Stability Test ===" | tee "$LOG_FILE"
echo "Test runs:       $TEST_RUNS" | tee -a "$LOG_FILE"
echo "Test directory:  $TEST_DIR" | tee -a "$LOG_FILE"
if [ -n "$TEST_PATTERN" ]; then
    echo "Target Test:     $TEST_PATTERN" | tee -a "$LOG_FILE"
else
    echo "Target Test:     ALL TESTS" | tee -a "$LOG_FILE"
fi
echo "Start time:      $(date)" | tee -a "$LOG_FILE"
echo "Usage: $0 [runs] [directory] [test_pattern]" | tee -a "$LOG_FILE"
echo "================================================" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Initialize counters
passed_count=0
failed_count=0
total_duration=0

# Change to test directory
cd "$TEST_DIR"

# Run tests
for i in $(seq 1 $TEST_RUNS); do
    echo "=== Test Run $i/$TEST_RUNS ===" | tee -a "$LOG_FILE"
    
    # Record start time
    start_time=$(date +%s)
    
    # Run the test and capture output
    # We use eval or if/else to handle the optional flag cleanly
    if [ -n "$TEST_PATTERN" ]; then
        # Run specific test case
        # -v: verbose
        # -count=1: disable cache
        # -run: matches specific test function/sub-test name
        if go test -count=1 -v -run "$TEST_PATTERN" ./... >> "$LOG_FILE" 2>&1; then
            RESULT="PASSED"
        else
            RESULT="FAILED"
        fi
    else
        # Run all tests
        if go test -count=1 -v ./... >> "$LOG_FILE" 2>&1; then
            RESULT="PASSED"
        else
            RESULT="FAILED"
        fi
    fi

    # Calculate duration
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    total_duration=$((total_duration + duration))

    # Log result
    if [ "$RESULT" == "PASSED" ]; then
        passed_count=$((passed_count + 1))
        echo "Run $i: PASSED (${duration}s)" | tee -a "$LOG_FILE"
    else
        failed_count=$((failed_count + 1))
        echo "Run $i: FAILED (${duration}s)" | tee -a "$LOG_FILE"
        # Optional: Stop on first failure if you want to debug immediately
        # echo "Stopping on first failure." | tee -a "$LOG_FILE"
        # break
    fi
    
    echo "" | tee -a "$LOG_FILE"
    
    # Progress indicator
    progress=$((i * 100 / TEST_RUNS))
    # Print progress to stderr so it doesn't clutter the log file if redirecting
    echo -ne "Progress: $progress% ($i/$TEST_RUNS) - Passed: $passed_count, Failed: $failed_count\r" >&2
done

echo "" >&2 # Newline after progress bar

# Final summary
echo "" | tee -a "$LOG_FILE"
echo "=== FINAL SUMMARY ===" | tee -a "$LOG_FILE"
echo "Target Test:  ${TEST_PATTERN:-ALL}" | tee -a "$LOG_FILE"
echo "Total runs:   $TEST_RUNS" | tee -a "$LOG_FILE"
echo "Passed:       $passed_count" | tee -a "$LOG_FILE"
echo "Failed:       $failed_count" | tee -a "$LOG_FILE"
if [ $TEST_RUNS -gt 0 ]; then
    echo "Success rate: $(echo "scale=2; $passed_count * 100 / $TEST_RUNS" | bc)%" | tee -a "$LOG_FILE"
    echo "Average time: $(echo "scale=2; $total_duration / $TEST_RUNS" | bc)s" | tee -a "$LOG_FILE"
fi
echo "End time:     $(date)" | tee -a "$LOG_FILE"

# Create summary file
cat > "$SUMMARY_FILE" << EOF
Test Summary Report
==================
Target: ${TEST_PATTERN:-ALL}
Total runs: $TEST_RUNS
Passed: $passed_count
Failed: $failed_count
Success rate: $(if [ $TEST_RUNS -gt 0 ]; then echo "scale=2; $passed_count * 100 / $TEST_RUNS" | bc; else echo 0; fi)%
Total time: ${total_duration}s
Average time per run: $(if [ $TEST_RUNS -gt 0 ]; then echo "scale=2; $total_duration / $TEST_RUNS" | bc; else echo 0; fi)s
Test directory: $TEST_DIR
Generated: $(date)
EOF

if [ $failed_count -eq 0 ]; then
    echo -e "\n\033[0;32mAll tests passed successfully.\033[0m"
else
    echo -e "\n\033[0;31mSome tests failed ($failed_count failures). Check the logs for details.\033[0m"
fi

echo "Detailed logs saved to: $LOG_FILE"