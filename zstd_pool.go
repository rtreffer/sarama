package sarama

import (
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
		p.idleEncoders = p.idleEncoders[i:]
		return
	}
	// the whole slice expired
	p.idleEncoders = p.idleEncoders[:0]
}

func (p *zstdEncoderSettingPools) getPool(params ZstdEncoderParams) *zstdEncoderPool {
	if pool, found := ((*sync.Map)(p)).Load(params); found {
		return pool.(*zstdEncoderPool)
	}
	pool := &zstdEncoderPool{
		lock:         &sync.Mutex{},
		params:       params,
		idleEncoders: make([]zstdEncoderPoolEntry, 0, runtime.GOMAXPROCS(0)),
		channel:      make(chan *zstd.Encoder, 1),
		baselineTTL:  1 * time.Second,
	}
	fetched, _ := ((*sync.Map)(p)).LoadOrStore(params, pool)
	return fetched.(*zstdEncoderPool)
}

func (p *zstdEncoderPool) getZstdEncoder() *zstd.Encoder {
	p.lock.Lock()

	// check if we have idle encoders and reuse the first one, then run cleanup
	if len(p.idleEncoders) > 0 {
		encoder := p.idleEncoders[0]
		p.idleEncoders = p.idleEncoders[1:]
		p.outstandingEncoders++
		p.cleanup()
		p.lock.Unlock()

		return encoder.encoder
	}

	// we couldn't reuse an encoder, perform cleanup
	p.cleanup()

	// GOMAXPROCS can be changed at runtime, so check before create
	outstandingLimit := runtime.GOMAXPROCS(0)

	if outstandingLimit > p.outstandingEncoders {
		p.outstandingEncoders++
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
		p.cleanup()
		p.lock.Unlock()
		return
	}

	if p.waitingRoutines == 0 {
		// nothing is waiting so our encoder should become idle
		p.outstandingEncoders--
		p.idleEncoders = append(p.idleEncoders, zstdEncoderPoolEntry{
			encoder: enc,
			// Our expiry is `1 + number of idle encoders` seconds
			// This ensures that if we get a big influx of encoders (say 8) we will expire them over 9s
			// The amortized allocations / deallocations per second are thus 1
			expire: time.Now().Add(p.baselineTTL*time.Duration(len(p.idleEncoders)) + p.baselineTTL),
		})
		p.cleanup()
		p.lock.Unlock()
		return
	}

	p.cleanup()
	p.lock.Unlock()

	// reshare our encoder
	p.channel <- enc
}
