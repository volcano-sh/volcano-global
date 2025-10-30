#!/bin/bash

# Stability Test Script for Go Projects
# This script runs tests multiple times and logs the results for stability analysis

set -e

# Configuration
TEST_RUNS=${1:-50}  # Default to 50 runs, can be overridden by first argument
TEST_DIR=${2:-$(pwd)}  # Default to current directory, can be overridden by second argument
LOG_FILE="$TEST_DIR/stability_test_${TEST_RUNS}_runs.log"
SUMMARY_FILE="$TEST_DIR/test_summary.log"

# Initialize counters

echo "=== Go Project Stability Test ===" | tee "$LOG_FILE"
echo "Test runs: $TEST_RUNS" | tee -a "$LOG_FILE"
echo "Test directory: $TEST_DIR" | tee -a "$LOG_FILE"
echo "Start time: $(date)" | tee -a "$LOG_FILE"
echo "Usage: $0 [test_runs] [test_directory]" | tee -a "$LOG_FILE"
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
    if go test -v ./... >> "$LOG_FILE" 2>&1; then
        end_time=$(date +%s)
        duration=$((end_time - start_time))
        total_duration=$((total_duration + duration))
        passed_count=$((passed_count + 1))
        echo "Run $i: PASSED (${duration}s)" | tee -a "$LOG_FILE"
    else
        end_time=$(date +%s)
        duration=$((end_time - start_time))
        total_duration=$((total_duration + duration))
        failed_count=$((failed_count + 1))
        echo "Run $i: FAILED (${duration}s)" | tee -a "$LOG_FILE"
    fi
    
    echo "" | tee -a "$LOG_FILE"
    
    # Progress indicator
    progress=$((i * 100 / TEST_RUNS))
    echo "Progress: $progress% ($i/$TEST_RUNS)"
done

# Final summary
echo "" | tee -a "$LOG_FILE"
echo "=== FINAL SUMMARY ===" | tee -a "$LOG_FILE"
echo "Total runs: $TEST_RUNS" | tee -a "$LOG_FILE"
echo "Passed: $passed_count" | tee -a "$LOG_FILE"
echo "Failed: $failed_count" | tee -a "$LOG_FILE"
echo "Success rate: $(echo "scale=2; $passed_count * 100 / $TEST_RUNS" | bc)%" | tee -a "$LOG_FILE"
echo "Total time: ${total_duration}s" | tee -a "$LOG_FILE"
echo "Average time per run: $(echo "scale=2; $total_duration / $TEST_RUNS" | bc)s" | tee -a "$LOG_FILE"
echo "End time: $(date)" | tee -a "$LOG_FILE"

# Create summary file
cat > "$SUMMARY_FILE" << EOF
Test Summary Report
==================
Total runs: $TEST_RUNS
Passed: $passed_count
Failed: $failed_count
Success rate: $(echo "scale=2; $passed_count * 100 / $TEST_RUNS" | bc)%
Total time: ${total_duration}s
Average time per run: $(echo "scale=2; $total_duration / $TEST_RUNS" | bc)s
Test directory: $TEST_DIR
Generated: $(date)
EOF

if [ $failed_count -eq 0 ]; then
    echo "All tests passed successfully."
else
    echo "Some tests failed. Check the logs for details."
fi

echo "Detailed logs saved to: $LOG_FILE"
echo "Summary saved to: $SUMMARY_FILE"