package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gotd/td/tg"
	range_parser "github.com/quantumsheep/range-parser"
	"github.com/apm1432/worker/botworker"
	"github.com/apm1432/worker/stream"
	"github.com/apm1432/worker/tgutil"
	workertoken "github.com/apm1432/worker/token"
	"go.uber.org/zap"
)

var log *zap.Logger

func setupRoutes(r *gin.Engine, l *zap.Logger) {
	log = l.Named("handler")
	r.GET("/ping", handlePing)
	r.GET("/worker-stream/:workerToken", handleWorkerStream)
}

// /ping
func handlePing(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"ok":      true,
		"mode":    "worker",
		"time":    time.Now().UTC().Format(time.RFC3339),
		"workers": botworker.Count(),
	})
}

// /worker-stream/:workerToken
func handleWorkerStream(c *gin.Context) {
	w := c.Writer
	r := c.Request

	// 1. Token verify
	verified, err := workertoken.Verify(c.Param("workerToken"))
	if err != nil {
		switch err {
		case workertoken.ErrTokenExpired:
			http.Error(w, "worker stream token expired", http.StatusForbidden)
		case workertoken.ErrWrongType:
			http.Error(w, "invalid token type for worker endpoint", http.StatusForbidden)
		default:
			http.Error(w, "invalid worker stream token", http.StatusForbidden)
		}
		return
	}

	// 2. Block direct browser navigation (address-bar paste / devtools open).
	// A legitimate <video> element sets Sec-Fetch-Dest: video (or audio/empty for
	// range sub-requests). Address-bar navigation always sets Sec-Fetch-Dest: document.
	// This header is browser-enforced — JavaScript cannot spoof it.
	if r.Header.Get("Sec-Fetch-Dest") == "document" {
		http.Error(w, "Direct stream access is not allowed.", http.StatusForbidden)
		return
	}

	// 3. Bot worker निवडा (round-robin)
	botWorker := botworker.Next()
	if botWorker == nil {
		http.Error(w, "no bot workers available", http.StatusServiceUnavailable)
		return
	}

	log.Sugar().Debugf("worker-stream: msg=%d chan=%d bot=@%s",
		verified.MessageID, verified.ChannelID, botWorker.Self.Username)

	// 4. File metadata from Telegram
	file, err := tgutil.FileFromMessageInChannel(c, botWorker.Client, verified.MessageID, verified.ChannelID, log)
	if err != nil {
		log.Error("file fetch failed", zap.Error(err))
		http.Error(w, "could not load file from Telegram: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// 5. Photo (FileSize == 0) — chunk by chunk download
	if file.FileSize == 0 {
		data, err := downloadPhoto(c, botWorker, file)
		if err != nil {
			log.Error("photo download failed", zap.Error(err))
			http.Error(w, "failed to load photo: "+err.Error(), http.StatusInternalServerError)
			return
		}
		c.Header("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"", file.FileName))
		c.Header("Content-Length", strconv.Itoa(len(data)))
		setAntiDownloadHeaders(c)
		if r.Method != "HEAD" {
			c.Data(http.StatusOK, file.MimeType, data)
		}
		return
	}

	// 6. Range-aware streaming (video, PDF, documents)
	c.Header("Accept-Ranges", "bytes")
	var start, end int64
	rangeHeader := r.Header.Get("Range")

	if rangeHeader == "" {
		start = 0
		end = file.FileSize - 1
		w.WriteHeader(http.StatusOK)
	} else {
		ranges, err := range_parser.Parse(file.FileSize, rangeHeader)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		start = ranges[0].Start
		end = ranges[0].End
		c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, file.FileSize))
		log.Debug("range request", zap.Int64("start", start), zap.Int64("end", end))
		w.WriteHeader(http.StatusPartialContent)
	}

	contentLength := end - start + 1
	mimeType := file.MimeType
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	c.Header("Content-Type", mimeType)
	c.Header("Content-Length", strconv.FormatInt(contentLength, 10))
	c.Header("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"", file.FileName))
	setAntiDownloadHeaders(c)

	if r.Method == "HEAD" {
		return
	}

	// 7. Pipe: Telegram → Browser
	pipe, err := stream.NewStreamPipe(c, botWorker.Client, file.Location, start, end, log)
	if err != nil {
		log.Error("stream pipe create failed", zap.Error(err))
		return
	}
	defer pipe.Close()

	if _, err := io.CopyN(w, pipe, contentLength); err != nil {
		if !tgutil.IsClientDisconnectError(err) {
			log.Error("stream copy error", zap.Error(err))
		}
	}
}

// downloadPhoto downloads a photo by fetching chunks until done.
// Handles both upload.file and upload.fileCdnRedirect responses.
func downloadPhoto(c *gin.Context, botWorker *botworker.Worker, file *tgutil.File) ([]byte, error) {
	const chunkSize = 512 * 1024 // 512 KB per chunk
	var buf bytes.Buffer
	var offset int64

	for {
		res, err := botWorker.Client.API().UploadGetFile(c, &tg.UploadGetFileRequest{
			Location: file.Location,
			Offset:   offset,
			Limit:    chunkSize,
			Precise:  true,
		})
		if err != nil {
			return nil, fmt.Errorf("UploadGetFile offset=%d: %w", offset, err)
		}

		switch r := res.(type) {
		case *tg.UploadFile:
			chunk := r.GetBytes()
			if len(chunk) == 0 {
				// No more data
				return buf.Bytes(), nil
			}
			buf.Write(chunk)
			offset += int64(len(chunk))
			if len(chunk) < chunkSize {
				// Last chunk
				return buf.Bytes(), nil
			}

		case *tg.UploadFileCDNRedirect:
			// CDN redirect — download directly from CDN URL
			return downloadFromCDN(r)

		default:
			return nil, fmt.Errorf("unexpected UploadGetFile response type %T", res)
		}
	}
}

// downloadFromCDN fetches photo data from Telegram CDN.
func downloadFromCDN(redirect *tg.UploadFileCDNRedirect) ([]byte, error) {
	// CDN URL: https://<dc_id>.cdn.telegram.org/file/<file_token>
	url := fmt.Sprintf("https://cdn%d.telegram.org/file/%s",
		redirect.DCID,
		string(redirect.FileToken),
	)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("CDN fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("CDN returned status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("CDN read: %w", err)
	}
	return data, nil
}

func setAntiDownloadHeaders(c *gin.Context) {
	c.Header("Cache-Control", "no-store, no-cache, must-revalidate")
	c.Header("Pragma", "no-cache")
	c.Header("X-Content-Type-Options", "nosniff")
}
