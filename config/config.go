package config

import (
	"log"
	"path/filepath"

	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

var C = &Config{}

type Config struct {
	// Server
	Port int  `envconfig:"PORT" default:"8080"`
	Dev  bool `envconfig:"DEV"  default:"false"`

	// Must match main server's STREAM_SECRET — used to decrypt incoming tokens
	StreamSecret string `envconfig:"STREAM_SECRET" required:"true"`

	// Main server URL — worker startup ला इथे ping/register करतो
	MainServerURL string `envconfig:"MAIN_SERVER_URL" required:"true"`

	// Stream tuning (optional — sensible defaults)
	StreamConcurrency int `envconfig:"STREAM_CONCURRENCY" default:"5"`
	StreamBufferCount int `envconfig:"STREAM_BUFFER_COUNT" default:"25"`
	StreamTimeoutSec  int `envconfig:"STREAM_TIMEOUT_SEC"  default:"20"`
	StreamMaxRetries  int `envconfig:"STREAM_MAX_RETRIES"  default:"3"`
}

func Load() {
	_ = godotenv.Load(filepath.Clean("worker.env"))

	if err := envconfig.Process("", C); err != nil {
		log.Fatalf("[config] env parse failed: %v", err)
	}

	validate()
}

func validate() {
	if C.StreamConcurrency <= 0 {
		C.StreamConcurrency = 5
	}
	if C.StreamBufferCount <= 0 {
		C.StreamBufferCount = 25
	}
	if C.StreamTimeoutSec <= 0 {
		C.StreamTimeoutSec = 20
	}
	if C.StreamMaxRetries <= 0 {
		C.StreamMaxRetries = 3
	}
}
