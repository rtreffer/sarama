package sarama

import (
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
)

type zstdEncoderPool struct {
	lock                *sync.Mutex
	params              ZstdEncoderParams
	idleEncoders        []zstdEncoderPoolEntry
	outstandingEncoders int
	waitingRoutines     int
	channel             chan *zstd.Encoder
	baselineTTL         time.Duration
	runningEncoderLimit int
	maxIdleEncoders     int

	// statistics

	// encoderCreationCount is the number of encoders created
	encoderCreationCount int
	// encoderReuseCount is the number of encoders reused directly (not going idle)
	encoderReuseDirectCount int
	// encoderReuseIdleCount is the number of encoders reused from the idle pool
	encoderReuseIdleCount int
	// encoderDropIdleCount is the number of encoders dropped from idle state
	encoderDropIdleCount int
	// encoderDropDirectCount is the number of encoders dropped directly due to max idle or outstanding encoder limits
	encoderDropDirectCount int
	// encoderDropWaitCount is the number of times a goroutine had to wait for an encoder
	encoderWaitCount int
}

type zstdEncoderPoolEntry struct {
	expire  time.Time
	encoder *zstd.Encoder
}

type zstdEncoderSettingPools sync.Map

func (p *zstdEncoderPool) cleanup() {
	now := time.Now()
	for i, e := range p.idleEncoders {
		if e.expire.Before(now) {
			continue
		}
		p.encoderDropIdleCount += i
		p.idleEncoders = p.idleEncoders[i:]
		return
	}

	// the whole slice expired
	p.encoderDropIdleCount += len(p.idleEncoders)
	p.idleEncoders = p.idleEncoders[:0]
}

func (p *zstdEncoderPool) reset() {
	p.lock.Lock()
	// we can't reset encoderWaitCount or outstandingEncoders as it may cause deadlocks otherwise
	// reset the idle encoders as well as the statistics
	p.idleEncoders = p.idleEncoders[:0]
	p.encoderCreationCount = 0
	p.encoderReuseDirectCount = 0
	p.encoderReuseIdleCount = 0
	p.encoderDropIdleCount = 0
	p.encoderDropDirectCount = 0
	p.lock.Unlock()
}

func (p *zstdEncoderSettingPools) getPool(params ZstdEncoderParams) *zstdEncoderPool {
	if pool, found := ((*sync.Map)(p)).Load(params); found {
		return pool.(*zstdEncoderPool)
	}
	pool := &zstdEncoderPool{
		lock:            &sync.Mutex{},
		params:          params,
		idleEncoders:    make([]zstdEncoderPoolEntry, 0, runtime.GOMAXPROCS(0)),
		channel:         make(chan *zstd.Encoder, 1),
		baselineTTL:     1 * time.Second,
		maxIdleEncoders: math.MaxInt,
	}
	fetched, _ := ((*sync.Map)(p)).LoadOrStore(params, pool)
	return fetched.(*zstdEncoderPool)
}

func (p *zstdEncoderPool) getOutstandingEncoderLimit() int {
	if p.runningEncoderLimit == 0 {
		return runtime.GOMAXPROCS(0)
	}
	return p.runningEncoderLimit
}

func (p *zstdEncoderPool) getZstdEncoder() *zstd.Encoder {
	p.lock.Lock()

	// check if we have idle encoders and reuse the first one, then run cleanup
	if len(p.idleEncoders) > 0 {
		encoder := p.idleEncoders[0]
		p.idleEncoders = p.idleEncoders[1:]
		p.outstandingEncoders++
		p.encoderReuseIdleCount++
		p.cleanup()
		p.lock.Unlock()

		return encoder.encoder
	}

	// we couldn't reuse an encoder, perform cleanup
	p.cleanup()

	// GOMAXPROCS can be changed at runtime, so check before create
	outstandingLimit := p.getOutstandingEncoderLimit()

	if outstandingLimit > p.outstandingEncoders {
		p.outstandingEncoders++
		p.encoderCreationCount++
		p.lock.Unlock()

		encoderLevel := zstd.SpeedDefault
		if p.params.Level != CompressionLevelDefault {
			encoderLevel = zstd.EncoderLevelFromZstd(p.params.Level)
		}
		zstdEnc, _ := zstd.NewWriter(nil, zstd.WithZeroFrames(true),
			zstd.WithEncoderLevel(encoderLevel),
			zstd.WithEncoderConcurrency(1))

		return zstdEnc
	}

	// we have reached GOMAXPROCS, let's wait until an encoder becomes available
	p.encoderWaitCount++
	p.waitingRoutines++
	p.lock.Unlock()

	return <-p.channel
}

func (p *zstdEncoderSettingPools) getZstdEncoder(params ZstdEncoderParams) *zstd.Encoder {
	return p.getPool(params).getZstdEncoder()
}

func (p *zstdEncoderSettingPools) releaseEncoder(params ZstdEncoderParams, enc *zstd.Encoder) {
	p.getPool(params).releaseEncoder(enc)
}

func (p *zstdEncoderPool) releaseEncoder(enc *zstd.Encoder) {
	p.lock.Lock()

	// check if we are above GOMAXPROCS
	// in that case we should just drop our encoder
	// this can happen if GOMAXPROCS was reduced

	outstandingLimit := runtime.GOMAXPROCS(0)
	if outstandingLimit < p.outstandingEncoders {
		p.outstandingEncoders--
		p.encoderDropDirectCount++
		p.cleanup()
		p.lock.Unlock()
		return
	}

	if p.waitingRoutines == 0 {
		// nothing is waiting so our encoder should become idle
		p.outstandingEncoders--
		cleanupDone := false
		if len(p.idleEncoders) >= p.maxIdleEncoders {
			cleanupDone = true
			p.cleanup()
		}
		if len(p.idleEncoders) < p.maxIdleEncoders {
			p.idleEncoders = append(p.idleEncoders, zstdEncoderPoolEntry{
				encoder: enc,
				// Our expiry is `1 + number of idle encoders` seconds
				// This ensures that if we get a big influx of encoders (say 8) we will expire them over 9s
				// The amortized allocations / deallocations per second are thus 1
				expire: time.Now().Add(p.baselineTTL*time.Duration(len(p.idleEncoders)) + p.baselineTTL),
			})
		} else {
			p.encoderDropDirectCount++
		}
		if !cleanupDone {
			p.cleanup()
		}
		p.lock.Unlock()
		return
	}

	p.encoderReuseDirectCount++
	p.cleanup()
	p.lock.Unlock()

	// reshare our encoder
	p.channel <- enc
}
