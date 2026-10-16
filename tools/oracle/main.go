// Command oracle serves envoyproxy/ratelimit's own ShouldRateLimit implementation, backed by
// Redis, with a clock and config that tests control. It is the reference that the Rust
// implementation is compared against (docs/design.md D32).
//
// HTTP control endpoints (on -control):
//
//	PUT /time    body: unix seconds          sets the clock
//	PUT /config  body: YAML config file      loads a config (as one file named "config.yaml");
//	                                         422 with the error when it does not load
//	GET /stats                               flushes the stats; returns the service's counters
//	                                         so far, one "name value" per line. With -statsd,
//	                                         the flush also reaches the statsd server, followed
//	                                         by a "harness.flushed:1|c" line; with -dogstatsd,
//	                                         the DogStatsD server, the same way.
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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coocood/freecache"
	pb "github.com/envoyproxy/go-control-plane/envoy/service/ratelimit/v3"
	gostats "github.com/lyft/gostats"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"

	"github.com/envoyproxy/ratelimit/src/config"
	"github.com/envoyproxy/ratelimit/src/godogstats"
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

// freecacheTimer gives freecache the controlled clock.
type freecacheTimer struct{ clock *clock }

func (t freecacheTimer) Now() uint32 { return uint32(t.clock.UnixNow()) }

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

// countingSink adds up the counter deltas the store flushes into it.
type countingSink struct {
	mu       sync.Mutex
	counters map[string]uint64
}

func (s *countingSink) FlushCounter(name string, value uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counters[name] += value
}
func (s *countingSink) FlushGauge(name string, value uint64)  {}
func (s *countingSink) FlushTimer(name string, value float64) {}

// teeSink flushes to both sinks.
type teeSink struct {
	counting *countingSink
	upstream gostats.Sink
}

func (s *teeSink) FlushCounter(name string, value uint64) {
	s.counting.FlushCounter(name, value)
	s.upstream.FlushCounter(name, value)
}
func (s *teeSink) FlushGauge(name string, value uint64) {
	s.counting.FlushGauge(name, value)
	s.upstream.FlushGauge(name, value)
}
func (s *teeSink) FlushTimer(name string, value float64) {
	s.counting.FlushTimer(name, value)
	s.upstream.FlushTimer(name, value)
}
func (s *teeSink) Flush() {
	// Like gostats' store: only a FlushableSink is flushed (the DogStatsD sink is not).
	if flushable, ok := s.upstream.(gostats.FlushableSink); ok {
		flushable.Flush()
	}
}

func hostAndPort(address string) (string, int) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		log.Fatal(err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil {
		log.Fatal(err)
	}
	return host, portNumber
}

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
	localCacheSize := flag.Int("local-cache-size", 0, "LOCAL_CACHE_SIZE_IN_BYTES (0 disables the local cache)")
	stopIncrement := flag.Bool("stop-cache-key-increment", false, "STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT")
	nearLimitRatio := flag.Float64("near-limit-ratio", 0.8, "NEAR_LIMIT_RATIO")
	statsdAddr := flag.String("statsd", "", "host:port of a statsd server (TCP) that also receives the stats")
	dogstatsdAddr := flag.String("dogstatsd", "", "host:port of a DogStatsD server (UDP) that also receives the stats")
	flag.Parse()

	ctx := context.Background()
	sink := &countingSink{counters: map[string]uint64{}}
	var storeSink gostats.Sink = sink
	// The sink upstream's runner builds for USE_STATSD or USE_DOG_STATSD, if any.
	var upstreamSink gostats.Sink
	if *statsdAddr != "" {
		host, port := hostAndPort(*statsdAddr)
		upstreamSink = gostats.NewTCPStatsdSink(gostats.WithStatsdHost(host), gostats.WithStatsdPort(port),
			gostats.WithStatsdProtocol("tcp"))
	}
	if *dogstatsdAddr != "" {
		host, port := hostAndPort(*dogstatsdAddr)
		dog, err := godogstats.NewSink(godogstats.WithStatsdHost(host), godogstats.WithStatsdPort(port),
			godogstats.WithMogrifierFromEnv(settings.NewSettings().UseDogStatsdMogrifiers))
		if err != nil {
			log.Fatal(err)
		}
		upstreamSink = dog
	}
	if upstreamSink != nil {
		storeSink = &teeSink{counting: sink, upstream: upstreamSink}
	}
	store := gostats.NewStore(storeSink, false)
	statsManager := stats.NewStatManager(store, settings.NewSettings())
	clk := &clock{}

	client := redis.NewClientImpl(ctx, store.Scope("redis"), false, "", "tcp", "SINGLE", *redisURL, 10,
		0, 0, nil, false, nil, 10*time.Second, "", "", time.Second, time.Second, 10*time.Second, false)
	// As in the upstream runner: no local cache unless a size is configured.
	var localCache *freecache.Cache
	if *localCacheSize > 0 {
		// The upstream runner uses freecache's real-time timer; the controlled clock is the
		// same thing for a test, and keeps eviction (which compares access times) deterministic.
		localCache = freecache.NewCacheCustomTimer(*localCacheSize, freecacheTimer{clk})
	}
	cache := redis.NewFixedRateLimitCacheImpl(client, nil, clk, rand.New(utils.NewLockedSource(1)), 0,
		localCache, float32(*nearLimitRatio), *prefix, statsManager, *stopIncrement, false)

	// Nothing is sent on it: PUT /config calls SetConfig directly. NewService needs a provider.
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
		// Applied before answering, so the stats read next include it.
		svc.SetConfig(&configEvent{config: cfg, err: loadErr}, false)
		if loadErr != nil {
			http.Error(w, fmt.Sprint(loadErr), http.StatusUnprocessableEntity)
		}
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		store.Flush()
		if upstreamSink != nil {
			upstreamSink.FlushCounter("harness.flushed", 1)
			if flushable, ok := upstreamSink.(gostats.FlushableSink); ok {
				flushable.Flush()
			}
		}
		sink.mu.Lock()
		defer sink.mu.Unlock()
		names := make([]string, 0, len(sink.counters))
		for name := range sink.counters {
			if strings.HasPrefix(name, "ratelimit.service.") || name == "reserved_tag" {
				names = append(names, name)
			}
		}
		sort.Strings(names)
		for _, name := range names {
			fmt.Fprintf(w, "%s %d\n", name, sink.counters[name])
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
