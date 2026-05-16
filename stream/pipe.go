package stream

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/celestix/gotgproto"
	"github.com/gotd/td/tg"
	"github.com/apm1432/worker/config"
	"go.uber.org/zap"
)

// calculateBlockSize determines optimal block size based on the range requested.
func calculateBlockSize(start, end int64) int64 {
	size := end - start + 1
	switch {
	case size < 512*1024:
		return 64 * 1024
	case size < 4*1024*1024:
		return 256 * 1024
	case size < 32*1024*1024:
		return 512 * 1024
	default:
		return 1024 * 1024
	}
}

// StreamPipe reads data from Telegram with concurrent prefetching.
// Implements io.ReadCloser.
type StreamPipe struct {
	ctx    context.Context
	cancel context.CancelFunc
	log    *zap.Logger

	client   *gotgproto.Client
	location tg.InputFileLocationClass

	start      int64
	end        int64
	blockSize  int64
	totalBytes int64

	blockQueue   chan []byte
	currentBlock []byte
	blockOffset  int64
	bytesRead    int64

	closeOnce sync.Once
}

func NewStreamPipe(
	ctx context.Context,
	client *gotgproto.Client,
	location tg.InputFileLocationClass,
	start, end int64,
	log *zap.Logger,
) (io.ReadCloser, error) {
	if start > end {
		return nil, fmt.Errorf("invalid range: start (%d) > end (%d)", start, end)
	}

	ctx, cancel := context.WithCancel(ctx)
	totalBytes := end - start + 1
	blockSize := calculateBlockSize(start, end)

	p := &StreamPipe{
		ctx:        ctx,
		cancel:     cancel,
		log:        log.Named("StreamPipe"),
		client:     client,
		location:   location,
		start:      start,
		end:        end,
		blockSize:  blockSize,
		totalBytes: totalBytes,
		blockQueue: make(chan []byte, config.C.StreamBufferCount),
	}

	go p.prefetch()
	return p, nil
}

func (p *StreamPipe) Read(buf []byte) (n int, err error) {
	if p.bytesRead >= p.totalBytes {
		return 0, io.EOF
	}

	if p.blockOffset >= int64(len(p.currentBlock)) {
		select {
		case block, ok := <-p.blockQueue:
			if !ok {
				if p.bytesRead >= p.totalBytes {
					return 0, io.EOF
				}
				return 0, ErrPipeDrained
			}
			p.currentBlock = block
			p.blockOffset = 0
		case <-p.ctx.Done():
			return 0, p.ctx.Err()
		}
	}

	n = copy(buf, p.currentBlock[p.blockOffset:])
	p.blockOffset += int64(n)
	p.bytesRead += int64(n)
	return n, nil
}

func (p *StreamPipe) Close() error {
	p.closeOnce.Do(func() { p.cancel() })
	return nil
}

func (p *StreamPipe) prefetch() {
	defer close(p.blockQueue)

	alignedStart := p.start - (p.start % p.blockSize)
	leftTrim := p.start - alignedStart
	rightTrim := (p.end % p.blockSize) + 1
	totalBlocks := int((p.end - alignedStart + p.blockSize) / p.blockSize)

	currentBlock := 0
	offset := alignedStart

	for currentBlock < totalBlocks {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		batchSize := min(config.C.StreamConcurrency, totalBlocks-currentBlock)
		blocks := make([][]byte, batchSize)

		var wg sync.WaitGroup
		var fetchErr error
		var errMu sync.Mutex

		for i := range batchSize {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				blockNum := currentBlock + idx
				blockOffset := offset + int64(idx)*p.blockSize

				data, err := p.downloadBlockWithRetry(blockOffset)
				if err != nil {
					errMu.Lock()
					if fetchErr == nil {
						fetchErr = err
					}
					errMu.Unlock()
					return
				}

				dataLen := int64(len(data))
				if totalBlocks == 1 {
					if dataLen < rightTrim {
						rightTrim = dataLen
					}
					if leftTrim > dataLen {
						leftTrim = dataLen
					}
					data = data[leftTrim:rightTrim]
				} else if blockNum == 0 {
					if leftTrim > dataLen {
						leftTrim = dataLen
					}
					data = data[leftTrim:]
				} else if blockNum == totalBlocks-1 {
					if dataLen > rightTrim {
						data = data[:rightTrim]
					}
				}

				blocks[idx] = data
			}(i)
		}

		wg.Wait()

		if fetchErr != nil {
			if p.ctx.Err() == nil {
				p.log.Error("block download failed", zap.Error(fetchErr))
			}
			return
		}

		for _, block := range blocks {
			if block == nil {
				p.log.Error("unexpected nil block, aborting prefetch")
				return
			}
			select {
			case p.blockQueue <- block:
			case <-p.ctx.Done():
				return
			}
		}

		currentBlock += batchSize
		offset += p.blockSize * int64(batchSize)
	}
}

func (p *StreamPipe) downloadBlockWithRetry(offset int64) ([]byte, error) {
	var lastErr error
	backoff := 100 * time.Millisecond
	const maxBackoff = 15 * time.Second

	for attempt := 0; attempt < config.C.StreamMaxRetries; attempt++ {
		if p.ctx.Err() != nil {
			return nil, p.ctx.Err()
		}

		ctx, cancel := context.WithTimeout(p.ctx, time.Duration(config.C.StreamTimeoutSec)*time.Second)
		data, err := p.downloadBlock(ctx, offset)
		cancel()

		if err == nil {
			return data, nil
		}
		lastErr = err

		if p.ctx.Err() != nil {
			return nil, p.ctx.Err()
		}

		select {
		case <-time.After(backoff):
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		case <-p.ctx.Done():
			return nil, p.ctx.Err()
		}
	}

	return nil, fmt.Errorf("%w: %v", ErrMaxRetriesExceeded, lastErr)
}

func (p *StreamPipe) downloadBlock(ctx context.Context, offset int64) ([]byte, error) {
	res, err := p.client.API().UploadGetFile(ctx, &tg.UploadGetFileRequest{
		Offset:   offset,
		Limit:    int(p.blockSize),
		Location: p.location,
	})
	if err != nil {
		return nil, err
	}

	switch result := res.(type) {
	case *tg.UploadFile:
		return result.Bytes, nil
	case *tg.UploadFileCDNRedirect:
		return nil, fmt.Errorf("CDN redirect not supported (DC %d)", result.DCID)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", res)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
