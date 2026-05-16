package main

import (
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
//
// Security:
//   1. AES-256-GCM token decrypt (same STREAM_SECRET as main server)
//   2. Token expiry check
//   3. SessionToken == "" check (worker tokens मध्ये हे empty असतं)
//
// Session / nonce / access — main server च्या handleWatch ने आधीच validate केलं.
// Worker फक्त pipe करतो: Telegram → Browser.
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

	// 2. Bot worker निवडा (Level 2 round-robin)
	botWorker := botworker.Next()
	if botWorker == nil {
		http.Error(w, "no bot workers available", http.StatusServiceUnavailable)
		return
	}

	log.Sugar().Debugf("worker-stream: msg=%d chan=%d bot=@%s",
		verified.MessageID, verified.ChannelID, botWorker.Self.Username)

	// 3. File metadata from Telegram
	file, err := tgutil.FileFromMessageInChannel(c, botWorker.Client, verified.MessageID, verified.ChannelID, log)
	if err != nil {
		log.Error("file fetch failed", zap.Error(err))
		http.Error(w, "could not load file from Telegram", http.StatusInternalServerError)
		return
	}

	// 4. Small file / photo
	if file.FileSize == 0 {
		res, err := botWorker.Client.API().UploadGetFile(c, &tg.UploadGetFileRequest{
			Location: file.Location,
			Offset:   0,
			Limit:    1024 * 1024,
		})
		if err != nil {
			http.Error(w, "failed to load file", http.StatusInternalServerError)
			return
		}
		result, ok := res.(*tg.UploadFile)
		if !ok {
			http.Error(w, "unexpected file response", http.StatusInternalServerError)
			return
		}
		c.Header("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"", file.FileName))
		setAntiDownloadHeaders(c)
		if r.Method != "HEAD" {
			c.Data(http.StatusOK, file.MimeType, result.GetBytes())
		}
		return
	}

	// 5. Range-aware streaming
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

	// 6. Pipe: Telegram → Browser
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

func setAntiDownloadHeaders(c *gin.Context) {
	c.Header("Cache-Control", "no-store, no-cache, must-revalidate")
	c.Header("Pragma", "no-cache")
	c.Header("X-Content-Type-Options", "nosniff")
}
