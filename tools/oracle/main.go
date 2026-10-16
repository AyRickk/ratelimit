// Command oracle serves envoyproxy/ratelimit's own ShouldRateLimit implementation, backed by
// Redis, with a clock and config that tests control. It is the reference that the Rust
// implementation is compared against (docs/design.md D32).
//
// HTTP control endpoints (on -control):
//
//	PUT /time    body: unix seconds          sets the clock
//	PUT /config  body: YAML config file      loads a config (as one file named "config.yaml")
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	pb "github.com/envoyproxy/go-control-plane/envoy/service/ratelimit/v3"
	gostats "github.com/lyft/gostats"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"

	"github.com/envoyproxy/ratelimit/src/config"
	"github.com/envoyproxy/ratelimit/src/provider"
	"github.com/envoyproxy/ratelimit/src/redis"
	"github.com/envoyproxy/ratelimit/src/server"
	ratelimit "github.com/envoyproxy/ratelimit/src/service"
	"github.com/envoyproxy/ratelimit/src/settings"
	"github.com/envoyproxy/ratelimit/src/stats"
	"github.com/envoyproxy/ratelimit/src/utils"
)

type clock struct{ now atomic.Int64 }

func (c *clock) UnixNow() int64 { return c.now.Load() }

// configEvents delivers configs pushed through the control endpoint to the service.
type configEvents struct {
	events chan provider.ConfigUpdateEvent
}

func (p *configEvents) ConfigUpdateEvent() <-chan provider.ConfigUpdateEvent { return p.events }
func (p *configEvents) Stop()                                                {}

type configEvent struct {
	config config.RateLimitConfig
	err    any
}

func (e *configEvent) GetConfig() (config.RateLimitConfig, any) { return e.config, e.err }

func loadConfig(content string, statsManager stats.Manager) (cfg config.RateLimitConfig, err any) {
	defer func() {
		if r := recover(); r != nil {
			cfg, err = nil, r
		}
	}()
	root := config.ConfigFileContentToYaml("config.yaml", content)
	files := []config.RateLimitConfigToLoad{{Name: "config.yaml", ConfigYaml: root}}
	return config.NewRateLimitConfigImpl(files, statsManager, false), nil
}

func main() {
	grpcAddr := flag.String("grpc", "127.0.0.1:0", "gRPC listen address")
	controlAddr := flag.String("control", "127.0.0.1:0", "control HTTP listen address")
	redisURL := flag.String("redis", "127.0.0.1:6379", "Redis address")
	prefix := flag.String("cache-key-prefix", "", "CACHE_KEY_PREFIX")
	flag.Parse()

	ctx := context.Background()
	store := gostats.NewStore(gostats.NewNullSink(), false)
	statsManager := stats.NewStatManager(store, settings.NewSettings())
	clk := &clock{}

	client := redis.NewClientImpl(ctx, store.Scope("redis"), false, "", "tcp", "SINGLE", *redisURL, 10,
		0, 0, nil, false, nil, 10*time.Second, "", "", time.Second, time.Second, 10*time.Second, false)
	cache := redis.NewFixedRateLimitCacheImpl(client, nil, clk, rand.New(utils.NewLockedSource(1)), 0,
		nil, 0.8, *prefix, statsManager, false, false)

	events := &configEvents{events: make(chan provider.ConfigUpdateEvent)}
	health := server.NewHealthChecker(health.NewServer(), "ratelimit", false)
	// forceStart: serve before any config is loaded, like FORCE_START_WITHOUT_INITIAL_CONFIG.
	svc := ratelimit.NewService(cache, events, statsManager, health, clk, false, true, false)

	grpcListener, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterRateLimitServiceServer(grpcServer, svc)
	go func() { log.Fatal(grpcServer.Serve(grpcListener)) }()

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /time", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		now, err := strconv.ParseInt(strings.TrimSpace(string(body)), 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		clk.now.Store(now)
	})
	mux.HandleFunc("PUT /config", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		cfg, loadErr := loadConfig(string(body), statsManager)
		events.events <- &configEvent{config: cfg, err: loadErr}
		if loadErr != nil {
			http.Error(w, fmt.Sprint(loadErr), http.StatusUnprocessableEntity)
		}
	})
	controlListener, err := net.Listen("tcp", *controlAddr)
	if err != nil {
		log.Fatal(err)
	}
	// The test harness reads these two lines to find the listeners.
	fmt.Printf("grpc %s\ncontrol %s\n", grpcListener.Addr(), controlListener.Addr())
	log.Fatal(http.Serve(controlListener, mux))
}
