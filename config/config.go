package config

import (
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

var C = &Config{}

type Config struct {
	// Telegram
	ApiID    int32  `envconfig:"API_ID"   required:"true"`
	ApiHash  string `envconfig:"API_HASH" required:"true"`
	BotToken string `envconfig:"BOT_TOKEN" required:"true"`

	// Server
	Port int  `envconfig:"PORT" default:"8080"`
	Dev  bool `envconfig:"DEV"  default:"false"`

	// Stream tuning
	StreamConcurrency int `envconfig:"STREAM_CONCURRENCY" default:"4"`
	StreamBufferCount int `envconfig:"STREAM_BUFFER_COUNT" default:"8"`
	StreamTimeoutSec  int `envconfig:"STREAM_TIMEOUT_SEC"  default:"30"`
	StreamMaxRetries  int `envconfig:"STREAM_MAX_RETRIES"  default:"3"`

	// Security — must be SAME as main server
	// Generate: openssl rand -hex 32
	StreamSecret string `envconfig:"STREAM_SECRET" required:"true"`

	// Reuse session files across restarts (faster startup)
	SessionFile bool `envconfig:"USE_SESSION_FILE" default:"true"`

	// Extra bot tokens for Level 2 round-robin (optional)
	// MULTI_TOKEN1=xxx, MULTI_TOKEN2=xxx ...
	MultiTokens []string
}

func (c *Config) UseSessionFile() bool {
	return c.SessionFile
}

func Load() {
	// .env file load करा (नसल्यास ignore)
	envPath := filepath.Clean("worker.env")
	_ = godotenv.Load(envPath)

	if err := envconfig.Process("", C); err != nil {
		log.Fatalf("[config] failed to parse env: %v", err)
	}

	// MULTI_TOKEN1, MULTI_TOKEN2... collect करा
	for _, env := range os.Environ() {
		if strings.HasPrefix(env, "MULTI_TOKEN") {
			parts := strings.SplitN(env, "=", 2)
			if len(parts) == 2 && parts[1] != "" {
				C.MultiTokens = append(C.MultiTokens, parts[1])
			}
		}
	}

	validate()
}

func validate() {
	if C.StreamConcurrency <= 0 {
		C.StreamConcurrency = 4
	}
	if C.StreamBufferCount <= 0 {
		C.StreamBufferCount = 8
	}
	if C.StreamTimeoutSec <= 0 {
		C.StreamTimeoutSec = 30
	}
	if C.StreamMaxRetries <= 0 {
		C.StreamMaxRetries = 3
	}
}
