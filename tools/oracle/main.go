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
//	PUT /mapper  body: statsd_exporter mapper YAML    loads it into a standalone mapper (not the
//	                                         sink's), as upstream's sink would (without a
//	                                         logger); 422 with the error when it does not load,
//	                                         500 when loading it panics
//	POST /map    body: "<type> <name>" lines  maps each name with that mapper; returns one JSON
//	                                         object per line
//
// With -prometheus, the stats also go to upstream's Prometheus sink, served on that address:
//
//	POST /flush  body: "<counter|gauge|timer> <name> <value>" lines
//	                                         flushes them straight into the sink
//	PUT /exporter-time  body: unix nanoseconds
//	                                         sets statsd_exporter's clock, then lets it remove
//	                                         stale series
//
// GET /stats and POST /flush answer once the sink's exporter has handled every event.
//
// With -runtime-snapshot, the oracle only builds goruntime's loader as upstream's file provider
// does from the RUNTIME_* variables, then prints its first snapshot ({"snapshot": {key:
// base64}, "counters": {name: value}}) or its error ({"error": "..."}), and exits.
//
// With -file-provider, the service gets its config from upstream's file provider (RUNTIME_*
// variables) instead of PUT /config.
//
// With -settings, the oracle only prints upstream's settings.NewSettings() for its environment
// as JSON ({"settings": {VARIABLE: value}, "sink": failure}, durations in nanoseconds, floats
// as strconv.FormatFloat(v, 'g', -1, 64) text, and how creating the stats sink fails, if it
// does), or the panic it raises ({"error": "..."}), and exits.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coocood/freecache"
	pb "github.com/envoyproxy/go-control-plane/envoy/service/ratelimit/v3"
	gostats "github.com/lyft/gostats"
	"github.com/prometheus/statsd_exporter/pkg/mapper"
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
	"github.com/envoyproxy/ratelimit/src/stats/prom"
	"github.com/envoyproxy/ratelimit/src/utils"
	"github.com/lyft/goruntime/loader"
	"github.com/prometheus/client_golang/prometheus"
	exporterclock "github.com/prometheus/statsd_exporter/pkg/clock"
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

// teeSink flushes to both sinks, counting what reaches the upstream one.
type teeSink struct {
	counting   *countingSink
	upstream   gostats.Sink
	prometheus bool
	sent       atomic.Int64
}

func (s *teeSink) FlushCounter(name string, value uint64) {
	s.counting.FlushCounter(name, value)
	if s.forward(name) {
		s.upstream.FlushCounter(name, value)
	}
}
func (s *teeSink) FlushGauge(name string, value uint64) {
	s.counting.FlushGauge(name, value)
	if s.forward(name) {
		s.upstream.FlushGauge(name, value)
	}
}
func (s *teeSink) FlushTimer(name string, value float64) {
	s.counting.FlushTimer(name, value)
	if s.forward(name) {
		s.upstream.FlushTimer(name, value)
	}
}

// forward tells whether a stat goes on to the upstream sink, counting it. The Prometheus sink
// does not get the Redis client's stats (redis.*), which the Rust implementation does not
// emit yet.
func (s *teeSink) forward(name string) bool {
	if s.prometheus && strings.HasPrefix(name, "redis.") {
		return false
	}
	s.sent.Add(1)
	return true
}

// handledEvents is how many events statsd_exporter has handled: each one counts once in
// exactly one of these families (or ends the process).
func handledEvents() int64 {
	families, _ := prometheus.DefaultGatherer.Gather()
	var total float64
	for _, family := range families {
		switch family.GetName() {
		case "statsd_exporter_events_total", "statsd_exporter_events_conflict_total",
			"statsd_exporter_events_error_total":
			for _, metric := range family.Metric {
				total += metric.GetCounter().GetValue()
			}
		case "statsd_exporter_events_actions_total":
			for _, metric := range family.Metric {
				for _, label := range metric.Label {
					if label.GetValue() == "drop" {
						total += metric.GetCounter().GetValue()
					}
				}
			}
		}
	}
	return int64(total)
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

// mapResult is what the mapper decided for one name, with every option of the mapping.
type mapResult struct {
	Matched          bool                     `json:"matched"`
	Name             string                   `json:"name"`
	Labels           map[string]string        `json:"labels"`
	Action           string                   `json:"action"`
	Help             string                   `json:"help"`
	HonorLabels      bool                     `json:"honor_labels"`
	ObserverType     string                   `json:"observer_type"`
	MatchType        string                   `json:"match_type"`
	MatchMetricType  string                   `json:"match_metric_type"`
	TtlNanos         int64                    `json:"ttl"`
	Scale            *float64                 `json:"scale"`
	HistogramOptions *mapper.HistogramOptions `json:"histogram_options"`
	SummaryOptions   *mapper.SummaryOptions   `json:"summary_options"`
}

// mapNames maps each "<type> <name>" line of body with m.
func mapNames(m *mapper.MetricMapper, body string) []mapResult {
	var results []mapResult
	for _, line := range strings.Split(body, "\n") {
		if line == "" {
			continue
		}
		metricType, name, _ := strings.Cut(line, " ")
		mapping, labels, matched := m.GetMapping(name, mapper.MetricType(metricType))
		result := mapResult{Matched: matched, Labels: labels}
		if mapping != nil {
			result.Name = mapping.Name
			result.Action = string(mapping.Action)
			result.Help = mapping.HelpText
			result.HonorLabels = mapping.HonorLabels
			result.ObserverType = string(mapping.ObserverType)
			result.MatchType = string(mapping.MatchType)
			result.MatchMetricType = string(mapping.MatchMetricType)
			result.TtlNanos = int64(mapping.Ttl)
			if mapping.Scale.Set {
				result.Scale = &mapping.Scale.Val
			}
			result.HistogramOptions = mapping.HistogramOptions
			result.SummaryOptions = mapping.SummaryOptions
		}
		results = append(results, result)
	}
	return results
}

// printRuntimeSnapshot prints the first snapshot of the loader upstream's file provider builds
// (FileProvider.setupRuntime), with the counters it leaves, or its error.
func printRuntimeSnapshot() {
	encoder := json.NewEncoder(os.Stdout)
	defer func() {
		if r := recover(); r != nil {
			_ = encoder.Encode(map[string]string{"error": fmt.Sprint(r)})
		}
	}()
	s := settings.NewSettings()
	sink := &countingSink{counters: map[string]uint64{}}
	store := gostats.NewStore(sink, false)
	stats.NewStatManager(store, s)
	opts := []loader.Option{loader.AllowDotFiles}
	if s.RuntimeIgnoreDotFiles {
		opts = []loader.Option{loader.IgnoreDotFiles}
	}
	var runtime loader.IFace
	var err error
	if s.RuntimeWatchRoot {
		runtime, err = loader.New2(s.RuntimePath, s.RuntimeSubdirectory,
			store.ScopeWithTags("runtime", s.ExtraTags),
			&loader.SymlinkRefresher{RuntimePath: s.RuntimePath}, opts...)
	} else {
		refresher := &loader.DirectoryRefresher{}
		refresher.WatchFileSystemOps(loader.Remove, loader.Write, loader.Create, loader.Chmod)
		runtime, err = loader.New2(filepath.Join(s.RuntimePath, s.RuntimeSubdirectory),
			s.RuntimeAppDirectory, store.ScopeWithTags("runtime", s.ExtraTags), refresher, opts...)
	}
	if err != nil {
		panic(err)
	}
	snapshot := map[string]string{}
	for _, key := range runtime.Snapshot().Keys() {
		snapshot[key] = base64.StdEncoding.EncodeToString([]byte(runtime.Snapshot().Get(key)))
	}
	store.Flush()
	_ = encoder.Encode(map[string]any{"snapshot": snapshot, "counters": sink.counters})
}

// printSettings prints settings.NewSettings() as JSON, or its panic.
func printSettings() {
	encoder := json.NewEncoder(os.Stdout)
	defer func() {
		if r := recover(); r != nil {
			_ = encoder.Encode(map[string]string{"error": fmt.Sprint(r)})
		}
	}()
	s := settings.NewSettings()
	values := map[string]any{}
	v := reflect.ValueOf(s)
	for i := 0; i < v.NumField(); i++ {
		key := v.Type().Field(i).Tag.Get("envconfig")
		if key == "" {
			continue
		}
		switch value := v.Field(i).Interface().(type) {
		case time.Duration:
			values[key] = int64(value)
		case float32:
			values[key] = strconv.FormatFloat(float64(value), 'g', -1, 64)
		case float64:
			values[key] = strconv.FormatFloat(value, 'g', -1, 64)
		case []string:
			values[key] = append([]string{}, value...)
		case map[string]string:
			if value == nil {
				value = map[string]string{}
			}
			values[key] = value
		default:
			values[key] = value
		}
	}
	_ = encoder.Encode(map[string]any{"settings": values, "sink": sinkError(s)})
}

// sinkError is how the runner's stats sink selection fails for s (its Fatalf messages or
// panics), or "" when it does not; the Prometheus sink, which would listen, is not created.
func sinkError(s settings.Settings) (failure string) {
	defer func() {
		if r := recover(); r != nil {
			failure = fmt.Sprint(r)
		}
	}()
	several := "Error: unable to use more than one stats sink at the same time. Set one of USE_DOG_STATSD, USE_STATSD, USE_PROMETHEUS."
	switch {
	case s.DisableStats:
	case s.UseDogStatsd:
		if s.UseStatsd || s.UsePrometheus {
			return several
		}
		if _, err := godogstats.NewSink(godogstats.WithStatsdHost(s.StatsdHost),
			godogstats.WithStatsdPort(s.StatsdPort),
			godogstats.WithMogrifierFromEnv(s.UseDogStatsdMogrifiers)); err != nil {
			return fmt.Sprintf("Failed to create dogstatsd sink: %v", err)
		}
	case s.UseStatsd:
		if s.UsePrometheus {
			return several
		}
		gostats.NewTCPStatsdSink(gostats.WithStatsdHost(s.StatsdHost), gostats.WithStatsdPort(s.StatsdPort))
	}
	return ""
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
	prometheusAddr := flag.String("prometheus", "", "PROMETHEUS_ADDR: also send the stats to the Prometheus sink, served there")
	prometheusPath := flag.String("prometheus-path", "/metrics", "PROMETHEUS_PATH")
	prometheusMapper := flag.String("prometheus-mapper", "", "PROMETHEUS_MAPPER_YAML")
	prometheusMs := flag.Bool("prometheus-ms", false, "PROMETHEUS_RESPONSE_TIME_AS_MILLISECONDS")
	printSettingsOnly := flag.Bool("settings", false, "print settings.NewSettings() as JSON and exit")
	printSnapshotOnly := flag.Bool("runtime-snapshot", false, "print the runtime loader's first snapshot as JSON and exit")
	useFileProvider := flag.Bool("file-provider", false, "load the config with upstream's file provider")
	flag.Parse()
	if *printSettingsOnly {
		printSettings()
		return
	}
	if *printSnapshotOnly {
		printRuntimeSnapshot()
		return
	}

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
	// statsd_exporter's clock, which PUT /exporter-time moves; set before the sink starts.
	var exporterClock *exporterclock.Clock
	if *prometheusAddr != "" {
		exporterClock = &exporterclock.Clock{Instant: time.Unix(0, 0), TickerCh: make(chan time.Time)}
		exporterclock.ClockInstance = exporterClock
		upstreamSink = prom.NewPrometheusSink(prom.WithAddr(*prometheusAddr), prom.WithPath(*prometheusPath),
			prom.WithMapperYamlPath(*prometheusMapper), prom.WithResponseTimeAsMilliseconds(*prometheusMs))
	}
	var tee *teeSink
	if upstreamSink != nil {
		tee = &teeSink{counting: sink, upstream: upstreamSink, prometheus: exporterClock != nil}
		storeSink = tee
	}
	// Waits until the exporter has handled every event sent to it.
	awaitExporter := func() {
		if exporterClock == nil {
			return
		}
		for handledEvents() < tee.sent.Load() {
			time.Sleep(time.Millisecond)
		}
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
	var configProvider provider.RateLimitConfigProvider = events
	if *useFileProvider {
		configProvider = provider.NewFileProvider(settings.NewSettings(), statsManager, store)
	}
	svc := ratelimit.NewService(cache, configProvider, statsManager, health, clk, false, true, false)

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
	standalone := &mapper.MetricMapper{}
	var standaloneMu sync.Mutex
	mux.HandleFunc("PUT /mapper", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		standaloneMu.Lock()
		defer standaloneMu.Unlock()
		standalone = &mapper.MetricMapper{}
		defer func() {
			// Like upstream's sink, which sets no logger: a deprecation warning panics.
			if r := recover(); r != nil {
				standalone = &mapper.MetricMapper{}
				http.Error(w, fmt.Sprint("panic: ", r), http.StatusInternalServerError)
			}
		}()
		if err := standalone.InitFromYAMLString(string(body)); err != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		}
	})
	mux.HandleFunc("POST /map", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		standaloneMu.Lock()
		defer standaloneMu.Unlock()
		encoder := json.NewEncoder(w)
		for _, result := range mapNames(standalone, string(body)) {
			_ = encoder.Encode(result)
		}
	})
	mux.HandleFunc("POST /flush", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		for _, line := range strings.Split(string(body), "\n") {
			fields := strings.Fields(line)
			if len(fields) != 3 {
				continue
			}
			value, err := strconv.ParseFloat(fields[2], 64)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			switch fields[0] {
			case "counter":
				tee.FlushCounter(fields[1], uint64(value))
			case "gauge":
				tee.FlushGauge(fields[1], uint64(value))
			case "timer":
				tee.FlushTimer(fields[1], value)
			}
		}
		awaitExporter()
	})
	mux.HandleFunc("PUT /exporter-time", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		nanos, err := strconv.ParseInt(strings.TrimSpace(string(body)), 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		exporterClock.Instant = time.Unix(0, nanos)
		// The exporter handles one tick at a time: once the second is taken, the first
		// removal is done.
		exporterClock.TickerCh <- exporterClock.Instant
		exporterClock.TickerCh <- exporterClock.Instant
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		store.Flush()
		awaitExporter()
		if exporterClock == nil && upstreamSink != nil {
			upstreamSink.FlushCounter("harness.flushed", 1)
			if flushable, ok := upstreamSink.(gostats.FlushableSink); ok {
				flushable.Flush()
			}
		}
		sink.mu.Lock()
		defer sink.mu.Unlock()
		names := make([]string, 0, len(sink.counters))
		for name := range sink.counters {
			if strings.HasPrefix(name, "ratelimit.service.") || strings.HasPrefix(name, "runtime.") || name == "reserved_tag" {
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
