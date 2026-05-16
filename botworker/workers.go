package botworker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/celestix/gotgproto"
	"github.com/celestix/gotgproto/sessionMaker"
	"github.com/glebarez/sqlite"
	"github.com/gotd/contrib/middleware/floodwait"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/telegram"
	"github.com/apm1432/worker/config"
	"go.uber.org/zap"
)

// ─── Worker ───────────────────────────────────────────────────────────────────

type Worker struct {
	ID     int
	Client *gotgproto.Client
	Self   *tg.User
}

func (w *Worker) String() string {
	return fmt.Sprintf("{Worker (%d|@%s)}", w.ID, w.Self.Username)
}

// ─── Pool ─────────────────────────────────────────────────────────────────────

type Pool struct {
	mu      sync.Mutex
	bots    []*Worker
	counter int32 // atomic round-robin
	log     *zap.Logger
}

var Default = &Pool{}

// Start initialises the pool: starts main bot + any MULTI_TOKEN workers.
func Start(log *zap.Logger) error {
	Default.log = log.Named("BotWorkers")

	// Main bot
	mainClient, err := startBot(log, config.C.BotToken, 0)
	if err != nil {
		return fmt.Errorf("main bot: %w", err)
	}
	Default.add(mainClient, 0)
	Default.log.Sugar().Infof("Main bot @%s loaded", mainClient.Self.Username)

	// Extra MULTI_TOKEN workers
	if len(config.C.MultiTokens) == 0 {
		Default.log.Info("No MULTI_TOKEN workers — using main bot only")
		return nil
	}

	if config.C.UseSessionFile() {
		if err := os.MkdirAll("sessions", os.ModePerm); err != nil {
			return fmt.Errorf("create sessions dir: %w", err)
		}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var startErr error

	for i, token := range config.C.MultiTokens {
		wg.Add(1)
		go func(idx int, tok string) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			done := make(chan struct {
				c   *gotgproto.Client
				err error
			}, 1)

			go func() {
				c, err := startBot(log, tok, idx+1)
				done <- struct {
					c   *gotgproto.Client
					err error
				}{c, err}
			}()

			select {
			case res := <-done:
				if res.err != nil {
					mu.Lock()
					startErr = res.err
					mu.Unlock()
					log.Error("failed to start worker", zap.Int("idx", idx+1), zap.Error(res.err))
				} else {
					Default.add(res.c, idx+1)
					log.Sugar().Infof("Worker bot @%s loaded (ID %d)", res.c.Self.Username, idx+1)
				}
			case <-ctx.Done():
				log.Error("worker bot timeout", zap.Int("idx", idx+1))
			}
		}(i, token)
	}

	wg.Wait()
	Default.log.Sugar().Infof("Bot pool ready: %d workers", len(Default.bots))
	return startErr
}

// Next returns the next bot worker in round-robin order.
func Next() *Worker {
	Default.mu.Lock()
	defer Default.mu.Unlock()
	if len(Default.bots) == 0 {
		return nil
	}
	idx := atomic.AddInt32(&Default.counter, 1)
	return Default.bots[int(idx)%len(Default.bots)]
}

func (p *Pool) add(client *gotgproto.Client, id int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.bots = append(p.bots, &Worker{
		ID:     id,
		Client: client,
		Self:   client.Self,
	})
}

// ─── Bot startup ──────────────────────────────────────────────────────────────

func startBot(log *zap.Logger, token string, idx int) (*gotgproto.Client, error) {
	var sessionType sessionMaker.SessionConstructor
	if config.C.UseSessionFile() {
		sessionPath := filepath.Join("sessions", fmt.Sprintf("worker-%d.session", idx))
		sessionType = sessionMaker.SqlSession(sqlite.Open(sessionPath))
	} else {
		sessionType = sessionMaker.SimpleSession()
	}

	return gotgproto.NewClient(
		int(config.C.ApiID),
		config.C.ApiHash,
		gotgproto.ClientTypeBot(token),
		&gotgproto.ClientOpts{
			Session:          sessionType,
			DisableCopyright: true,
			Middlewares:      floodMiddleware(log),
		},
	)
}

func floodMiddleware(log *zap.Logger) []telegram.Middleware {
	waiter := floodwait.NewSimpleWaiter().WithMaxWait(10 * time.Minute)
	return []telegram.Middleware{waiter}
}

// Bots returns a copy of the current worker slice (for stats).
func (p *Pool) Bots() []*Worker {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*Worker, len(p.bots))
	copy(out, p.bots)
	return out
}

// Count returns the number of active bot workers.
func Count() int {
	Default.mu.Lock()
	defer Default.mu.Unlock()
	return len(Default.bots)
}