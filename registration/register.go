package registration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/apm1432/worker/config"
	"go.uber.org/zap"
)

// RegisterRequest — worker startup ला main server ला पाठवतो
type RegisterRequest struct {
	WorkerURL string `json:"worker_url"` // "https://my-worker.koyeb.app"
}

// RegisterResponse — main server कडून येतो
// Bot credentials + STREAM_SECRET आधीच token मध्ये येतात,
// registration response मध्ये फक्त confirmation असतो
type RegisterResponse struct {
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
}

// HeartbeatRequest — periodic health ping
type HeartbeatRequest struct {
	WorkerURL string `json:"worker_url"`
	Status    string `json:"status"` // "active"
}

var log *zap.Logger

// Start registers this worker with main server and begins heartbeat loop.
// workerURL = "https://this-worker.koyeb.app" (publicly accessible URL)
func Start(l *zap.Logger) {
	log = l.Named("Registration")

	workerURL := workerSelfURL()
	if workerURL == "" {
		log.Warn("WORKER_SELF_URL not set — skipping registration (standalone mode)")
		return
	}

	// Startup registration — retry until success
	go func() {
		for {
			if err := register(workerURL); err != nil {
				log.Warn("registration failed, retrying in 10s", zap.Error(err))
				time.Sleep(10 * time.Second)
				continue
			}
			log.Sugar().Infof("Registered with main server at %s", config.C.MainServerURL)
			break
		}

		// Heartbeat every 25 seconds
		// Main server health check interval is 30s — हे आधी येतं म्हणजे miss होत नाही
		ticker := time.NewTicker(25 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if err := heartbeat(workerURL); err != nil {
				log.Warn("heartbeat failed", zap.Error(err))
			}
		}
	}()
}

// ─── Internal ─────────────────────────────────────────────────────────────────

func register(workerURL string) error {
	body, _ := json.Marshal(RegisterRequest{WorkerURL: workerURL})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		config.C.MainServerURL+"/api/worker/register", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("main server returned %d", resp.StatusCode)
	}

	var r RegisterResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return err
	}
	if !r.OK {
		return fmt.Errorf("registration rejected: %s", r.Message)
	}
	return nil
}

func heartbeat(workerURL string) error {
	body, _ := json.Marshal(HeartbeatRequest{
		WorkerURL: workerURL,
		Status:    "active",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		config.C.MainServerURL+"/api/worker/heartbeat", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("heartbeat: main server returned %d", resp.StatusCode)
	}
	return nil
}

// workerSelfURL reads WORKER_SELF_URL env var.
// Koyeb deploy मध्ये हे automatically set असतं (APP_URL).
func workerSelfURL() string {
	if u := os.Getenv("WORKER_SELF_URL"); u != "" {
		return u
	}
	// Koyeb APP_URL fallback
	if u := os.Getenv("APP_URL"); u != "" {
		return u
	}
	return ""
}
