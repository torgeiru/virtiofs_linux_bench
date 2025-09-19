#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#define FILE_SIZE (1024 * 1024)  // 1MiB
#define SMALL_CHUNK 100
#define MEDIUM_CHUNK 1024
#define LARGE_CHUNK (64 * 1024)

struct BenchmarkResult {
    int chunk_size;
    int run_number;
    double read_time_ms;
    double throughput_mbps;
};

// Get current time in microseconds
uint64_t get_time_us() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000;
}

void write_result(int data_fd, const struct BenchmarkResult* result) {
    char buffer[256];
    int len = snprintf(buffer, sizeof(buffer), "%d,%d,%.3f,%.3f\n",
                      result->chunk_size, result->run_number,
                      result->read_time_ms, result->throughput_mbps);
    
    ssize_t written = write(data_fd, buffer, len);
    if (written != len) {
        fprintf(stderr, "Error writing result to file\n");
        exit(1);
    }
}

void benchmark_chunk_size(int chunk_size, int data_fd) {
    void* buffer = malloc(chunk_size);
    if (buffer == NULL) {
        fprintf(stderr, "Could not allocate chunk buffer!\n");
        exit(1);
    }
    
    for (int run = 0; run < 30; run++) {
        // Open test file
        int test_fd = open("test_file.bin", O_RDONLY);
        if (test_fd == -1) {
            perror("Could not open test file");
            free(buffer);
            exit(1);
        }
        
        printf("File descriptor is %d\n", test_fd);
        
        uint64_t start_time = get_time_us();
        
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
        
        uint64_t end_time = get_time_us();
        
        // Close test file
        if (close(test_fd) == -1) {
            perror("Error closing test file");
        }
        
        printf("Read %d bytes\n", total_read);
        
        if (total_read != FILE_SIZE) {
            fprintf(stderr, "Could not read entire file! Read %d bytes, expected %d\n", 
                   total_read, FILE_SIZE);
            free(buffer);
            exit(1);
        }
        
        // Calculate metrics
        double read_time_ms = (end_time - start_time) / 1000.0;
        double throughput_mbps = (FILE_SIZE / (1024.0 * 1024.0)) / (read_time_ms / 1000.0);
        
        // Write result
        struct BenchmarkResult result = {chunk_size, run + 1, read_time_ms, throughput_mbps};
        write_result(data_fd, &result);
    }
    
    free(buffer);
}

int main() {
    // Open data output file
    int data_fd = open("benchmark_results.csv", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (data_fd == -1) {
        perror("Could not open output file");
        return 1;
    }
    
    // Write CSV header
    const char* header = "chunk_size,run_number,read_time_ms,throughput_mbps\n";
    ssize_t header_written = write(data_fd, header, strlen(header));
    if (header_written != (ssize_t)strlen(header)) {
        fprintf(stderr, "Error writing CSV header\n");
        close(data_fd);
        return 1;
    }
    
    printf("Starting sequential read benchmark...\n");
    
    // Benchmark small reads (100 bytes)
    printf("Testing small reads (100 bytes)...\n");
    benchmark_chunk_size(SMALL_CHUNK, data_fd);
    
    // Benchmark medium reads (1K)
    printf("Testing medium reads (1K)...\n");
    benchmark_chunk_size(MEDIUM_CHUNK, data_fd);
    
    // Benchmark large reads (64K)
    printf("Testing large reads (64K)...\n");
    benchmark_chunk_size(LARGE_CHUNK, data_fd);
    
    // Close output file
    if (close(data_fd) == -1) {
        perror("Error closing output file");
        return 1;
    }
    
    printf("Benchmark completed. Results saved to benchmark_results.csv\n");
    return 0;
}