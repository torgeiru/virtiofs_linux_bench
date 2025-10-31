#include <iostream>
#include <cstdlib>
#include <chrono>
#include <string>
#include <format>

#include <fcntl.h>  // For posix open flags
#include <unistd.h> // For posix read

#define FILE_SIZE (1024 * 1024 * 64) // 64 MiB
#define SMALL_CHUNK 100
#define MEDIUM_CHUNK 1024
#define INCREMENTAL 8192
#define INCREMENTAL_START 8192
#define LARGEST_CHUNK (256 * 1024)

struct BenchmarkResult {
    int chunk_size;
    int run_number;
    double read_time_ms;
    double throughput_mbps;
};

void write_result(int data_fd, const struct BenchmarkResult& result) {
    std::string results_str  = std::format("{},{},{:.3f},{:.3f}\n",
        result.chunk_size,
        result.run_number,
        result.read_time_ms,
        result.throughput_mbps
    );
    
    ssize_t written = write(data_fd, results_str.data(), results_str.size());
    if (written != results_str.size()) {
        std::cerr << "Error writing result to file\n";
        std::exit(EXIT_FAILURE);
    }
}

void benchmark_chunk_size(int chunk_size, int data_fd) {
    void* buffer = std::malloc(chunk_size);
    if (buffer == NULL) {
        std::cerr << "Could not allocate chunk buffer!\n";
        exit(1);
    }
    
    for (int run = 0; run < 30; run++) {
        // Open test file
        int test_fd = open("test_file.bin", O_RDONLY);
        if (test_fd == -1) {
            std::cerr << "Could not open test file\n";
            std::free(buffer);
            std::exit(EXIT_FAILURE);
        }
        
        std::cout << "File descriptor is %d\n" << test_fd;
        
        auto start = std::chrono::high_resolution_clock::now();
        
        // Read entire file in chunks
        int total_read = 0;
        ssize_t bytes_read;
        
        while (total_read < FILE_SIZE) {
            int to_read = (FILE_SIZE - total_read > chunk_size) ? 
                         chunk_size : (FILE_SIZE - total_read);
            
            bytes_read = read(test_fd, buffer, to_read);
            if (bytes_read <= 0) break;
            total_read += bytes_read;
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> start_stop_diff = end - start;

        // Close test file
        if (close(test_fd) == -1) {
            std::cerr << "Error closing test file\n";
            std::exit(EXIT_FAILURE);
        }
        
        std::cout << "Read " << total_read << "bytes\n";
        
        if (total_read != FILE_SIZE) {
            std::cerr << "Could not read entire file!\n";
            std::free(buffer);
            std::exit(EXIT_FAILURE);
        }
        
        // Calculate metrics
        double read_time_ms = start_stop_diff.count() * 1000.0;
        double throughput_mbps = (FILE_SIZE / (1024.0 * 1024.0)) / start_stop_diff.count();
        
        // Write result
        struct BenchmarkResult result = {chunk_size, run + 1, read_time_ms, throughput_mbps};
        write_result(data_fd, result);
    }
    
    std::free(buffer);
}

int main() {
    // Open data output file
    int data_fd = open("benchmark_results.csv", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (data_fd == -1) {
        perror("Could not open output file");
        return 1;
    }
    
    // Write CSV header
    std::string header = "chunk_size,run_number,read_time_ms,throughput_mbps\n";
    ssize_t header_written = write(data_fd, header.data(), header.size());
    if (header_written != header.size()) {
        fprintf(stderr, "Error writing CSV header\n");
        close(data_fd);
        return 1;
    }
    
    std::cout << "Starting sequential read benchmark...\n";
    
    // Benchmark small reads (100 bytes)
    std::cout << "Testing small reads (100 bytes)\n";
    benchmark_chunk_size(SMALL_CHUNK, data_fd);
    
    // Benchmark medium reads (1K)
    std::cout << "Testing medium reads (1K)\n";
    benchmark_chunk_size(MEDIUM_CHUNK, data_fd);
    
    // Benchmark large reads (64K)
    std::cout << "Testing incremental reads of 8KiB starting from 8KiB up to 256KiB\n";
    for (int incremental_chunk_size = INCREMENTAL_START; 
        incremental_chunk_size <= LARGEST_CHUNK; 
        incremental_chunk_size +=INCREMENTAL)
    {
        benchmark_chunk_size(incremental_chunk_size, data_fd);
    }  
    
    // Close output file
    if (close(data_fd) == -1) {
        std::cout << "Error closing output file\n";
        return 1;
    }
    
    std::cout << "Benchmark completed. Results saved to benchmark_results.csv\n";
    return 0;
}