package sarama

import (
	"fmt"
	"math"
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

func benchZstdCpuAndMemoryHighConcurrency(b *testing.B, allocs bool, gomaxprocs, goroutines, maxEncoders, maxIdleEncoders int) {
	gomaxprocsBackup := runtime.GOMAXPROCS(gomaxprocs)
	defer runtime.GOMAXPROCS(gomaxprocsBackup)

	if b.N < goroutines {
		goroutines = b.N
	}
	runs := (b.N / goroutines) + 1
	b.N = goroutines * (runs - 1)

	params := ZstdEncoderParams{Level: 3}
	blocksize := 2048

	// configure the pool
	zstdEncPool.getPool(params).reset()
	zstdEncPool.getPool(params).runningEncoderLimit = maxEncoders
	if maxIdleEncoders >= 0 {
		zstdEncPool.getPool(params).maxIdleEncoders = maxIdleEncoders
	} else {
		zstdEncPool.getPool(params).maxIdleEncoders = math.MaxInt
	}

	// prepare the data
	buf := make([][]byte, goroutines)
	for i := 0; i < goroutines; i++ {
		buf[i] = make([]byte, blocksize)
		if i == 0 {
			for j := 0; j < len(buf[i]); j++ {
				buf[i][j] = byte((j / 256) + (j * 257))
			}
			continue
		}
		copy(buf[i], buf[i-1][1:])
		buf[i][blocksize-1] = buf[i][blocksize-1] + 1
	}

	b.SetBytes(int64(blocksize))
	runtime.GC()

	encodersPrerun := zstdEncPool.getPool(params).encoderCreationCount
	encoderPrerunWaits := zstdEncPool.getPool(params).encoderWaitCount
	encoderReuse := zstdEncPool.getPool(params).encoderReuseDirectCount + zstdEncPool.getPool(params).encoderReuseIdleCount

	var encoderValues = make(chan int, goroutines)
	sumMaxEncoders := 0
	lock := &sync.Mutex{}

	for run := 0; run <= runs; run++ {
		if run == 1 {
			sumMaxEncoders = 0
			encodersPrerun = zstdEncPool.getPool(params).encoderCreationCount
			encoderPrerunWaits = zstdEncPool.getPool(params).encoderWaitCount
			encoderReuse = zstdEncPool.getPool(params).encoderReuseDirectCount + zstdEncPool.getPool(params).encoderReuseIdleCount
			if allocs {
				b.ReportAllocs()
			}
			b.ResetTimer()
		}

		var startBarrier sync.WaitGroup
		startBarrier.Add(goroutines)

		var encoderCount atomic.Int32

		for i := 0; i < goroutines; i++ {
			id := i
			go func(id int, buf []byte) {
				startBarrier.Done()
				startBarrier.Wait()

				encoder := getZstdEncoder(params)
				lock.Lock()
				encoderId := encoderCount.Add(1)
				lock.Unlock()

				_ = encoder.EncodeAll(buf, nil)

				lock.Lock()
				releaseEncoder(params, encoder)
				encoderCount.Add(-1)
				lock.Unlock()

				encoderValues <- int(encoderId)
			}(id, buf[id])
		}

		maxEncoders := 0
		for i := 0; i < goroutines; i++ {
			encoderCount := <-encoderValues
			if encoderCount > maxEncoders {
				maxEncoders = int(encoderCount)
			}
		}
		sumMaxEncoders += maxEncoders
	}

	b.StopTimer()

	runtime.GC()

	createdEncoders := zstdEncPool.getPool(params).encoderCreationCount
	b.ReportMetric(float64(createdEncoders-encodersPrerun)/float64(b.N), "new/op")
	encoderWaits := zstdEncPool.getPool(params).encoderWaitCount
	b.ReportMetric(float64(encoderWaits-encoderPrerunWaits)/float64(b.N), "waits/op")
	encoderReuses := zstdEncPool.getPool(params).encoderReuseDirectCount + zstdEncPool.getPool(params).encoderReuseIdleCount
	b.ReportMetric(float64(encoderReuses-encoderReuse)/float64(b.N), "reuses/op")
	b.ReportMetric(float64(sumMaxEncoders)/float64(runs), "open_encs")
}

func BenchmarkZstdCpuAndMemoryHighConcurrencyGOMAXPROCEncoderLimit(b *testing.B) {
	benchZstdCpuAndMemoryHighConcurrency(b, false, 1, 1000, 0, -1)
}

func BenchmarkZstdCpuAndMemoryHighConcurrencyGOMAXPROCEncoderLimitAlloc(b *testing.B) {
	benchZstdCpuAndMemoryHighConcurrency(b, true, 1, 1000, 0, -1)
}

func BenchmarkZstdCpuAndMemoryHighConcurrencyIdle1GOMAXPROCEncoderLimit(b *testing.B) {
	benchZstdCpuAndMemoryHighConcurrency(b, false, 1, 1000, 0, 1)
}

func BenchmarkZstdCpuAndMemoryHighConcurrencyIdle1GOMAXPROCEncoderLimitAlloc(b *testing.B) {
	benchZstdCpuAndMemoryHighConcurrency(b, true, 1, 1000, 0, 1)
}

func BenchmarkZstdCpuAndMemoryHighConcurrencyIdle1(b *testing.B) {
	benchZstdCpuAndMemoryHighConcurrency(b, false, 1, 1000, 1000, 1)
}
func BenchmarkZstdCpuAndMemoryHighConcurrencyIdle1Alloc(b *testing.B) {
	benchZstdCpuAndMemoryHighConcurrency(b, true, 1, 1000, 1000, 1)
}
func BenchmarkZstdCpuAndMemoryHighConcurrencyIdleMaxGOMAXPROCEncoderLimit(b *testing.B) {
	benchZstdCpuAndMemoryHighConcurrency(b, false, 1, 1000, 0, 1000)
}

func BenchmarkZstdCpuAndMemoryHighConcurrencyIdleMaxGOMAXPROCEncoderLimitAlloc(b *testing.B) {
	benchZstdCpuAndMemoryHighConcurrency(b, true, 1, 1000, 0, 1000)
}

func BenchmarkZstdCpuAndMemoryHighConcurrencyIdleMax(b *testing.B) {
	benchZstdCpuAndMemoryHighConcurrency(b, false, 1, 1000, 1000, 1000)
}
func BenchmarkZstdCpuAndMemoryHighConcurrencyIdleMaxAlloc(b *testing.B) {
	benchZstdCpuAndMemoryHighConcurrency(b, true, 1, 1000, 1000, 1000)
}
func BenchmarkZstdCpuAndMemoryHighConcurrencyGOMAXPROCS4GOMAXPROCEncoderLimit(b *testing.B) {
	benchZstdCpuAndMemoryHighConcurrency(b, false, 4, 1000, 0, -1)
}

func BenchmarkZstdCpuAndMemoryHighConcurrencyGOMAXPROCS4GOMAXPROCEncoderLimitAlloc(b *testing.B) {
	benchZstdCpuAndMemoryHighConcurrency(b, true, 4, 1000, 0, -1)
}

func BenchmarkZstdCpuAndMemoryHighConcurrencyGOMAXPROCS4Idle1GOMAXPROCEncoderLimit(b *testing.B) {
	benchZstdCpuAndMemoryHighConcurrency(b, false, 4, 1000, 0, 1)
}
func BenchmarkZstdCpuAndMemoryHighConcurrencyGOMAXPROCS4Idle1GOMAXPROCEncoderLimitAlloc(b *testing.B) {
	benchZstdCpuAndMemoryHighConcurrency(b, true, 4, 1000, 0, 1)
}

func BenchmarkZstdCpuAndMemoryHighConcurrencyGOMAXPROCS4Idle1(b *testing.B) {
	benchZstdCpuAndMemoryHighConcurrency(b, false, 4, 1000, 1000, 1)
}
func BenchmarkZstdCpuAndMemoryHighConcurrencyGOMAXPROCS4Idle1Alloc(b *testing.B) {
	benchZstdCpuAndMemoryHighConcurrency(b, true, 4, 1000, 1000, 1)
}

func BenchmarkZstdCpuAndMemoryHighConcurrencyGOMAXPROCS4IdleMaxGOMAXPROCEncoderLimit(b *testing.B) {
	benchZstdCpuAndMemoryHighConcurrency(b, false, 4, 1000, 0, 1000)
}

func BenchmarkZstdCpuAndMemoryHighConcurrencyGOMAXPROCS4IdleMaxGOMAXPROCEncoderLimitAlloc(b *testing.B) {
	benchZstdCpuAndMemoryHighConcurrency(b, true, 4, 1000, 0, 1000)
}

func BenchmarkZstdCpuAndMemoryHighConcurrencyGOMAXPROCS4IdleMax(b *testing.B) {
	benchZstdCpuAndMemoryHighConcurrency(b, false, 4, 1000, 1000, 1000)
}

func BenchmarkZstdCpuAndMemoryHighConcurrencyGOMAXPROCS4IdleMaxAlloc(b *testing.B) {
	benchZstdCpuAndMemoryHighConcurrency(b, true, 4, 1000, 1000, 1000)
}

// BenchmarkZstdEncoderCreation benchmarks the creation of a zstd encoders
// under high concurrency and low GOMAXPROCS. In an ideal scenario we would
// never have more than 1 encoder per GOMAXPROCS.
func TestZstdEncoderCreation(t *testing.T) {
	gomaxprocsBackup := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(gomaxprocsBackup)

	goroutines := 1000
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

			encoder := getZstdEncoder(ZstdEncoderParams{Level: 3})
			lock.Lock()
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
