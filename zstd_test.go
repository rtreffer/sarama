package sarama

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func BenchmarkZstdMemoryConsumption(b *testing.B) {
	params := ZstdEncoderParams{Level: 9}
	buf := make([]byte, 1024*1024)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte((i / 256) + (i * 257))
	}

	cpus := 96

	gomaxprocsBackup := runtime.GOMAXPROCS(cpus)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 2*cpus; j++ {
			_, _ = zstdCompress(params, nil, buf)
		}
		// drain the buffered encoder
		getZstdEncoder(params)
		// previously this would be achieved with
		// zstdEncMap.Delete(params)
	}
	runtime.GOMAXPROCS(gomaxprocsBackup)
}

// BenchmarkZstdEncoderCreation benchmarks the creation of a zstd encoders
// under high concurrency and low GOMAXPROCS. In an ideal scenario we would
// never have more than 1 encoder per GOMAXPROCS.
func TestZstdEncoderCreation(t *testing.T) {
	gomaxprocsBackup := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(gomaxprocsBackup)

	goroutines := 500
	blockSize := 1024
	var startBarrier sync.WaitGroup
	startBarrier.Add(goroutines)

	type result struct {
		goroutineID     int
		compressedBytes int
		encoders        int
	}

	encoderValues := make(chan result, goroutines)

	var encoders atomic.Int32

	lock := &sync.Mutex{}

	for i := 0; i < goroutines; i++ {
		id := i
		go func(id int) {
			// generate a 1kb buffer and fill it with random data
			buf := make([]byte, blockSize)
			for i := 0; i < len(buf); i++ {
				buf[i] = byte(i + id)
			}

			startBarrier.Done()
			startBarrier.Wait()

			lock.Lock()
			encoder := getZstdEncoder(ZstdEncoderParams{Level: 3})
			currentEncoders := encoders.Add(1)
			lock.Unlock()

			output := encoder.EncodeAll(buf, nil)
			len := len(output)

			lock.Lock()
			releaseEncoder(ZstdEncoderParams{Level: 3}, encoder)
			encoders.Add(-1)
			lock.Unlock()

			encoderValues <- result{id, len, int(currentEncoders)}

		}(id)
	}

	maxEncoderValues := 0
	maxEncoersGoroutineId := 0
	totalCompressedBytes := 0
	totalBytes := 0
	encodersSum := 0
	for i := 0; i < goroutines; i++ {
		result := <-encoderValues
		totalCompressedBytes += result.compressedBytes
		totalBytes += blockSize
		if result.encoders > maxEncoderValues {
			maxEncoderValues = result.encoders
			maxEncoersGoroutineId = result.goroutineID
		}
		encodersSum += result.encoders
	}

	fmt.Printf("GOMAXPROCS: %d\n", runtime.GOMAXPROCS(0))
	fmt.Printf("Encoders: %d\n", encoders.Load())
	fmt.Printf("Max encoders: %d, goroutine ID: %d\n", maxEncoderValues, maxEncoersGoroutineId)
	fmt.Printf("Total compressed bytes: %d, total bytes: %d\n", totalCompressedBytes, totalBytes)
	fmt.Printf("Compression ratio: %f\n", float64(totalCompressedBytes)/float64(totalBytes))
	fmt.Printf("Average encoders: %f\n", float64(encodersSum)/float64(goroutines))

	if maxEncoderValues > 1 {
		t.Fatalf("Expected at most 1 encoder per GOMAXPROCS, got %d", maxEncoderValues)
	}
}
