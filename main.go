package main

import (
	"fmt"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/apm1432/worker/botpool"
	"github.com/apm1432/worker/config"
	"github.com/apm1432/worker/registration"
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

	// 3. Bot pool init (session directory बनवतो)
	if err := os.MkdirAll("sessions", os.ModePerm); err != nil {
		logger.Fatal("sessions dir create failed", zap.Error(err))
	}
	botpool.Init(logger)
	logger.Info("Bot pool ready (on-demand from token credentials)")

	// 4. Register with main server + start heartbeat
	// Goroutine मध्ये — HTTP server आधी start होतो म्हणजे
	// main server च्या /ping ला response देता येतो
	registration.Start(logger)

	// 5. HTTP server
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
	if config.C.Dev {
		r.Use(gin.Logger())
	}

	setupRoutes(r, logger)
	return r
}

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
