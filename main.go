package main

import (
	"fmt"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/apm1432/worker/botworker"
	"github.com/apm1432/worker/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var startTime = time.Now()

func main() {
	// 1. Config
	config.Load()

	// 2. Logger
	logger := initLogger(config.C.Dev)
	defer logger.Sync()

	logger.Info("🚀 TG Stream Worker starting",
		zap.Int("port", config.C.Port),
		zap.Bool("dev", config.C.Dev),
	)

	// 3. Telegram bot workers (Level 2 round-robin)
	if err := botworker.Start(logger); err != nil {
		logger.Fatal("[main] botworker.Start failed", zap.Error(err))
	}

	// 4. HTTP server
	router := buildRouter(logger)
	addr := fmt.Sprintf(":%d", config.C.Port)
	logger.Info("Worker HTTP server ready", zap.String("addr", addr))

	if err := router.Run(addr); err != nil {
		logger.Fatal("server exited", zap.Error(err))
	}
}

func buildRouter(logger *zap.Logger) *gin.Engine {
	if config.C.Dev {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	r.Use(gin.Recovery())

	// Request logger (dev mode मध्ये verbose)
	if config.C.Dev {
		r.Use(gin.Logger())
	}

	setupRoutes(r, logger)
	return r
}

// ─── Logger ───────────────────────────────────────────────────────────────────

func initLogger(dev bool) *zap.Logger {
	var lvl zapcore.Level
	if dev {
		lvl = zapcore.DebugLevel
	} else {
		lvl = zapcore.InfoLevel
	}

	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncodeLevel = zapcore.CapitalColorLevelEncoder

	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(cfg),
		zapcore.AddSync(os.Stdout),
		lvl,
	)

	return zap.New(core, zap.AddStacktrace(zapcore.FatalLevel))
}