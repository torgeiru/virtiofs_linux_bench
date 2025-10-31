#!/usr/bin/env python3
"""
Sequential Read Benchmark Orchestration Script
Creates test files, runs benchmark, and generates performance plots.
"""

import os
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
from pathlib import Path

# Configuration
FILE_SIZE_MB = 1
FILE_SIZE_BYTES = FILE_SIZE_MB * 1024 * 1024
TEST_FILE_NAME = "test_file.bin"
RESULTS_FILE = "benchmark_results.csv"
BENCHMARK_ELF = "seq_read_bench.elf.bin"

def create_test_file():
    # Create a 1MiB test file with random data
    print(f"Creating {FILE_SIZE_MB}MiB test file: {TEST_FILE_NAME}")
    
    # Generate random data
    with open(TEST_FILE_NAME, "wb") as f:
        # Write in 64KB chunks to avoid memory issues
        chunk_size = 64 * 1024
        remaining = FILE_SIZE_BYTES
        
        while remaining > 0:
            write_size = min(chunk_size, remaining)
            # Create pseudo-random data (simple pattern to avoid /dev/urandom dependency)
            data = bytearray(write_size)
            for i in range(write_size):
                data[i] = (i * 73 + 17) & 0xFF  # Simple pseudo-random pattern
            f.write(data)
            remaining -= write_size

    print(f"Test file created successfully ({os.path.getsize(TEST_FILE_NAME)} bytes)")

def run_benchmark():
    """Execute the benchmark program."""
    print(f"Running benchmark: boot {BENCHMARK_ELF}")
    
    try:
        # Execute the benchmark
        print("Executing the benchmark 🍾")
        result = subprocess.run(["./seq_read_bench"], timeout=20)

        if result.returncode != 0:
            print(f"Benchmark failed with return code {result.returncode}")
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")
            return False
        
        print("Benchmark completed successfully")
        print(f"Output: {result.stdout}")
        return True
        
    except subprocess.TimeoutExpired:
        print("Benchmark timed out after 5 minutes")
        return False
    except Exception as e:
        print(f"Error running benchmark: {e}")
        return False

def load_and_validate_results():
    """Load benchmark results and validate data."""    
    try:
        df = pd.read_csv(RESULTS_FILE)
        print(f"Loaded {len(df)} benchmark results")
        
        # Validate expected columns
        expected_cols = ['chunk_size', 'run_number', 'read_time_ms', 'throughput_mbps']
        if not all(col in df.columns for col in expected_cols):
            print(f"Missing columns in results file. Expected: {expected_cols}")
            return None
        
        # Validate we have expected chunk sizes and run counts
        chunk_sizes = df['chunk_size'].unique()
        
        print(f"Found chunk sizes: {sorted(chunk_sizes)}")
        
        # Check run counts per chunk size
        for chunk in chunk_sizes:
            run_count = len(df[df['chunk_size'] == chunk])
            print(f"Chunk size {chunk}: {run_count} runs")
        return df
        
    except Exception as e:
        print(f"Error loading results: {e}")
        return None

def generate_plots(df):
    """Generate boxplots for benchmark results."""
    print("Generating performance plots...")
    
    # Set up the plotting style
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Create figure with subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # Prepare data with readable chunk size labels
    df_plot = df.copy()

    chunk_size_map = {
        100: "100B",
        1024: "1KiB"
    }
    for i in range(1, 33):
        size = i * 8192
        chunk_size_map[size] = f"{8*i}KiB"
    expected_chunk_sizes = [100, 1024]
    expected_chunk_sizes.extend([8192 * i for i in range(1, 33)])

    df_plot['chunk_label'] = df_plot['chunk_size'].map(chunk_size_map)
    
    # Plot 1: Read Time
    sns.boxplot(data=df_plot, x='chunk_label', y='read_time_ms', ax=ax1)
    ax1.set_title('Sequential Read Performance - Read Time', fontsize=14, fontweight='bold')
    ax1.set_xlabel('Chunk Size', fontsize=12)
    ax1.set_ylabel('Read Time (ms)', fontsize=12)
    ax1.grid(True, alpha=0.3)
    
    # Add statistics to read time plot
    for i, chunk in enumerate(expected_chunk_sizes):
        chunk_data = df[df['chunk_size'] == chunk]['read_time_ms']
        median_val = chunk_data.median()
        ax1.text(i, median_val, f'Med: {median_val:.1f}ms', 
                ha='center', va='bottom', fontweight='bold', 
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))
    
    # Plot 2: Throughput
    sns.boxplot(data=df_plot, x='chunk_label', y='throughput_mbps', ax=ax2)
    ax2.set_title('Sequential Read Performance - Throughput', fontsize=14, fontweight='bold')
    ax2.set_xlabel('Chunk Size', fontsize=12)
    ax2.set_ylabel('Throughput (MB/s)', fontsize=12)
    ax2.grid(True, alpha=0.3)
    
    # Add statistics to throughput plot
    for i, chunk in enumerate(expected_chunk_sizes):
        chunk_data = df[df['chunk_size'] == chunk]['throughput_mbps']
        median_val = chunk_data.median()
        ax2.text(i, median_val, f'Med: {median_val:.1f}MB/s', 
                ha='center', va='bottom', fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))
    
    plt.tight_layout()
    
    # Save the plot
    output_file = "read_benchmark_results.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Plot saved as: {output_file}")
    
    # Display summary statistics
    print("\n=== BENCHMARK SUMMARY ===")
    for chunk in expected_chunk_sizes:
        chunk_name = chunk_size_map[chunk]
        chunk_data = df[df['chunk_size'] == chunk]

        print(f"\n{chunk_name}:")
        print(f"  Read Time - Mean: {chunk_data['read_time_ms'].mean():.2f}ms, "
              f"Median: {chunk_data['read_time_ms'].median():.2f}ms, "
              f"Std: {chunk_data['read_time_ms'].std():.2f}ms")
        print(f"  Throughput - Mean: {chunk_data['throughput_mbps'].mean():.2f}MB/s, "
              f"Median: {chunk_data['throughput_mbps'].median():.2f}MB/s, "
              f"Std: {chunk_data['throughput_mbps'].std():.2f}MB/s")

def main():
    """Main orchestration function."""
    print("=== Sequential Read Benchmark Orchestration ===")

    try:
        # Step 1: Create test file system
        create_test_file()

        # Step 2: Run benchmark
        if not run_benchmark():
            print("Benchmark execution failed!")
            return 1
        
        # Step 3: Load and validate results
        df = load_and_validate_results()
        if df is None:
            print("Failed to load valid results!")
            return 1
        
        # Step 4: Generate plots
        generate_plots(df)
        
        print("\n=== Benchmark completed successfully! ===")
        print(f"Results saved in: fs/{RESULTS_FILE}")
        print(f"Plots saved as: read_benchmark_results.png")
        
        return 0
        
    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
