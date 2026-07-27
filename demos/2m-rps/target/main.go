// Demo target: a tiny dependency-free HTTP API that does almost no work, so
// the load generator — not the server — is the variable under test. Same idea
// as the benchmark harness target, embedded here so the demo is self-contained.
package main

import (
	"flag"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	jsonBody = []byte(`{"service":"demo-target","ok":true,"message":"hello from the 2M rps demo"}`)
	served   atomic.Uint64
)

func jsonHandler(w http.ResponseWriter, _ *http.Request) {
	served.Add(1)
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonBody)
}

func healthz(w http.ResponseWriter, _ *http.Request) {
	w.Write([]byte("ok"))
}

func stats(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(strconv.FormatUint(served.Load(), 10) + "\n"))
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("/json", jsonHandler)
	mux.HandleFunc("/healthz", healthz)
	mux.HandleFunc("/stats", stats)
	mux.HandleFunc("/", jsonHandler)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	log.Printf("demo-target listening on %s", *addr)
	log.Fatal(srv.ListenAndServe())
}
