package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gotd/td/tg"
	range_parser "github.com/quantumsheep/range-parser"
	"github.com/apm1432/worker/botpool"
	"github.com/apm1432/worker/config"
	"github.com/apm1432/worker/stream"
	workertoken "github.com/apm1432/worker/token"
	"go.uber.org/zap"
)

var log *zap.Logger

func setupRoutes(r *gin.Engine, l *zap.Logger) {
	log = l.Named("handler")
	r.GET("/ping", handlePing)
	r.GET("/worker-stream/:workerToken", handleWorkerStream)
}

// GET /ping — main server health check करतो हे endpoint
func handlePing(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"ok":          true,
		"mode":        "worker",
		"time":        time.Now().UTC().Format(time.RFC3339),
		"cached_bots": botpool.Count(),
	})
}

// startStreamHeartbeat launches a goroutine that polls main server every 5s.
// जर 2 consecutive checks fail झाले (key mismatch किंवा network error) तर
// cancel() call होतो → stream pipe बंद होतो → Telegram connection सुटतो.
//
// Parameters:
//   - ctx        : stream चा context (cancel झाला की goroutine थांबतो)
//   - cancel     : stream cancel करायचा function
//   - userID     : user चा hex ID (main server ला query करायला)
//   - streamKey  : या stream साठी issue केलेली unique key
func startStreamHeartbeat(ctx context.Context, cancel context.CancelFunc, userID, streamKey string) {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		failCount := 0
		client := &http.Client{Timeout: 4 * time.Second}
		checkURL := config.C.MainServerURL + "/api/worker/stream-check"

		for {
			select {
			case <-ctx.Done():
				// Stream already cancelled/finished — goroutine बंद करा
				return

			case <-ticker.C:
				active, err := checkStreamActive(client, checkURL, userID, streamKey)
				if err != nil || !active {
					failCount++
					log.Warn("stream heartbeat check failed",
						zap.String("user_id", userID),
						zap.Bool("active", active),
						zap.Int("fail_count", failCount),
						zap.Error(err),
					)
					if failCount >= 2 {
						log.Info("killing stream — 2 consecutive heartbeat failures",
							zap.String("user_id", userID),
						)
						cancel()
						return
					}
				} else {
					// Successful check — reset counter
					failCount = 0
				}
			}
		}
	}()
}

// checkStreamActive calls main server's /api/worker/stream-check endpoint.
// Returns (true, nil) if stream is still the latest active one for the user.
// Returns (false, nil) if main server responded but stream is no longer active.
// Returns (false, err) on network/parse error.
func checkStreamActive(client *http.Client, checkURL, userID, streamKey string) (bool, error) {
	req, err := http.NewRequest(http.MethodGet, checkURL, nil)
	if err != nil {
		return false, fmt.Errorf("build request: %w", err)
	}

	q := req.URL.Query()
	q.Set("uid", userID)
	q.Set("key", streamKey)
	req.URL.RawQuery = q.Encode()
	req.Header.Set("X-Worker-Secret", config.C.StreamSecret)

	resp, err := client.Do(req)
	if err != nil {
		return false, fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("status %d", resp.StatusCode)
	}

	var body struct {
		Active bool `json:"active"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return false, fmt.Errorf("decode: %w", err)
	}

	return body.Active, nil
}

// GET /worker-stream/:workerToken
func handleWorkerStream(c *gin.Context) {
	w := c.Writer
	r := c.Request

	// 1. Token verify — सगळी info token मध्येच आहे
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

	// 2. Direct browser navigation block
	if r.Header.Get("Sec-Fetch-Dest") == "document" {
		http.Error(w, "Direct stream access is not allowed.", http.StatusForbidden)
		return
	}

	// 3. Cancellable context — heartbeat goroutine याच ctx ला cancel करतो
	// जेणेकरून user ने नवीन stream सुरू केल्यावर हा stream instant बंद होईल.
	streamCtx, streamCancel := context.WithCancel(r.Context())
	defer streamCancel()

	// 4. Heartbeat goroutine — दर 5s ला main server ला check करतो.
	// StreamKey valid नसेल (user ने नवीन stream सुरू केला) तर
	// 2 consecutive failures नंतर streamCancel() call होतो.
	if verified.StreamKey != "" && config.C.MainServerURL != "" {
		startStreamHeartbeat(streamCtx, streamCancel, verified.UserID, verified.StreamKey)
	}

	// 5. Bot client — token मधील credentials वापरून (pool मधून cached/new)
	botClient, err := botpool.Get(verified.ApiID, verified.ApiHash, verified.BotToken)
	if err != nil {
		log.Error("bot client unavailable", zap.Error(err))
		http.Error(w, "bot client unavailable: "+err.Error(), http.StatusServiceUnavailable)
		return
	}

	log.Sugar().Debugf("worker-stream: msg=%d chan=%d bot=@%s",
		verified.MessageID, verified.ChannelID, botClient.Self.Username)

	// 6. File location — token मधूनच build करतो (Telegram query नाही!)
	location, isPhoto := buildLocation(verified)

	// 7. Photo (FileSize == 0)
	if isPhoto {
		data, err := downloadPhoto(c, botClient.API(), location)
		if err != nil {
			log.Error("photo download failed", zap.Error(err))
			http.Error(w, "failed to load photo: "+err.Error(), http.StatusInternalServerError)
			return
		}
		c.Header("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"", verified.FileName))
		c.Header("Content-Length", strconv.Itoa(len(data)))
		setAntiDownloadHeaders(c)
		if r.Method != "HEAD" {
			c.Data(http.StatusOK, verified.MimeType, data)
		}
		return
	}

	// 8. Range-aware streaming
	c.Header("Accept-Ranges", "bytes")
	var start, end int64
	rangeHeader := r.Header.Get("Range")

	if rangeHeader == "" {
		start = 0
		end = verified.FileSize - 1
		w.WriteHeader(http.StatusOK)
	} else {
		ranges, err := range_parser.Parse(verified.FileSize, rangeHeader)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		start = ranges[0].Start
		end = ranges[0].End
		c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, verified.FileSize))
		log.Debug("range request", zap.Int64("start", start), zap.Int64("end", end))
		w.WriteHeader(http.StatusPartialContent)
	}

	contentLength := end - start + 1
	mimeType := verified.MimeType
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	c.Header("Content-Type", mimeType)
	c.Header("Content-Length", strconv.FormatInt(contentLength, 10))
	c.Header("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"", verified.FileName))
	setAntiDownloadHeaders(c)

	if r.Method == "HEAD" {
		return
	}

	// 9. Pipe: Telegram → Browser — streamCtx वापरतो (heartbeat cancel करू शकतो)
	pipe, err := stream.NewStreamPipe(streamCtx, botClient, location, start, end, log)
	if err != nil {
		log.Error("stream pipe create failed", zap.Error(err))
		return
	}
	defer pipe.Close()

	if _, err := io.CopyN(w, pipe, contentLength); err != nil {
		if !isClientDisconnect(err) {
			log.Error("stream copy error", zap.Error(err))
		}
	}
}

// buildLocation builds tg.InputFileLocationClass from token data.
// Token मध्ये file info असल्यामुळे Telegram API call नाही.
func buildLocation(v *workertoken.VerifyResult) (tg.InputFileLocationClass, bool) {
	if v.FileType == "photo" {
		return &tg.InputPhotoFileLocation{
			ID:            v.FileID,
			AccessHash:    v.AccessHash,
			FileReference: v.FileReference,
			ThumbSize:     v.ThumbSize,
		}, true
	}

	// document (video, pdf, etc.)
	return &tg.InputDocumentFileLocation{
		ID:            v.FileID,
		AccessHash:    v.AccessHash,
		FileReference: v.FileReference,
	}, false
}

// downloadPhoto downloads a photo chunk by chunk.
func downloadPhoto(c *gin.Context, api *tg.Client, location tg.InputFileLocationClass) ([]byte, error) {
	const chunkSize = 512 * 1024
	var buf bytes.Buffer
	var offset int64

	for {
		res, err := api.UploadGetFile(c, &tg.UploadGetFileRequest{
			Location: location,
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
				return buf.Bytes(), nil
			}
			buf.Write(chunk)
			offset += int64(len(chunk))
			if len(chunk) < chunkSize {
				return buf.Bytes(), nil
			}
		case *tg.UploadFileCDNRedirect:
			return downloadFromCDN(r)
		default:
			return nil, fmt.Errorf("unexpected response type %T", res)
		}
	}
}

func downloadFromCDN(redirect *tg.UploadFileCDNRedirect) ([]byte, error) {
	url := fmt.Sprintf("https://cdn%d.telegram.org/file/%s",
		redirect.DCID, string(redirect.FileToken))
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("CDN fetch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("CDN returned %d", resp.StatusCode)
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

func isClientDisconnect(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for _, sub := range []string{"connection was aborted", "connection reset by peer", "broken pipe", "forcibly closed"} {
		if len(s) >= len(sub) {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}
