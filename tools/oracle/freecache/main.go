// Command freecache replays cache operations on github.com/coocood/freecache, the local cache
// envoyproxy/ratelimit uses, so tests can compare the Rust port against it.
//
// One operation per line on stdin:
//
//	N <size>               start a new cache of <size> bytes
//	S <key> <ttl> <now>    Set(key, empty value, ttl) at Unix second <now>
//	G <key> <now>          Get(key) at Unix second <now>; prints 1 (hit) or 0 (miss)
//	C                      prints C and the cache's counts: evacuate, expired, entry, average
//	                       access time, hit, miss, lookup, overwrite
package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/coocood/freecache"
)

type timer struct{ now uint32 }

func (t *timer) Now() uint32 { return t.now }

func atoi(s string) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	return n
}

func main() {
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 1<<20)
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()
	clock := &timer{}
	var cache *freecache.Cache
	for in.Scan() {
		f := strings.Fields(in.Text())
		switch f[0] {
		case "N":
			cache = freecache.NewCacheCustomTimer(int(atoi(f[1])), clock)
		case "S":
			clock.now = uint32(atoi(f[3]))
			_ = cache.Set([]byte(f[1]), []byte{}, int(atoi(f[2])))
		case "C":
			fmt.Fprintln(out, "C", cache.EvacuateCount(), cache.ExpiredCount(), cache.EntryCount(),
				cache.AverageAccessTime(), cache.HitCount(), cache.MissCount(), cache.LookupCount(),
				cache.OverwriteCount())
		case "G":
			clock.now = uint32(atoi(f[2]))
			if _, err := cache.Get([]byte(f[1])); err == nil {
				fmt.Fprintln(out, 1)
			} else {
				fmt.Fprintln(out, 0)
			}
		}
	}
	if err := in.Err(); err != nil {
		log.Fatal(err)
	}
}
