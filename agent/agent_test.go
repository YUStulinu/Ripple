package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseAndFormat(t *testing.T) {
	s, ok, err := ParseLine(`http_requests_total{method="post",path="C:\\x \"y\""} 1027 1395066363000`)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if s.Name != "http_requests_total" || s.Value != 1027 || s.TS != 1395066363000 || s.Labels[1][1] != `C:\x "y"` {
		t.Fatalf("%+v", s)
	}
	back, _, err := ParseLine(s.String())
	if err != nil || back.String() != s.String() {
		t.Fatalf("round trip: %v %q", err, back.String())
	}
	if _, ok, _ := ParseLine("# TYPE x counter"); ok {
		t.Fatal("comment parsed")
	}
	if _, _, err := ParseLine(`x{a=1} 2`); err == nil {
		t.Fatal("bad line accepted")
	}
	inf, _, _ := ParseLine("x +Inf")
	if inf.Value <= 1e308 {
		t.Fatal("+Inf")
	}
	l := s.WithLabels([2]string{"job", "j"}, [2]string{"method", "get"})
	if l.String() != `http_requests_total{job="j",method="get",path="C:\\x \"y\""} 1027 1395066363000` {
		t.Fatal(l.String())
	}
}

func TestScrapeLabelsSamples(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "# HELP a b\nreqs_total{code=\"200\"} 5\nreqs_total{code=\"500\"} 1\n")
	}))
	defer target.Close()
	a := NewAgent("http://unused", []Target{{"web", target.URL}}, false)
	got := a.Collect(context.Background(), time.UnixMilli(1000))
	var lines []string
	for _, s := range got {
		lines = append(lines, s.String())
	}
	all := strings.Join(lines, "\n")
	host := strings.TrimPrefix(target.URL, "http://")
	for _, want := range []string{
		`reqs_total{code="200",instance="` + host + `",job="web"} 5 1000`,
		`up{job="web",instance="` + target.URL + `"} 1 1000`,
		`ripple_agent_scrape_duration_seconds_count{job="agent",instance=`,
	} {
		if !strings.Contains(all, want) {
			t.Fatalf("missing %q in\n%s", want, all)
		}
	}
}

// The remote fails for a while: batches stay queued and arrive in order once it recovers.
func TestQueueSurvivesOutage(t *testing.T) {
	var mu sync.Mutex
	down := true
	var bodies []string
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if down {
			w.WriteHeader(503)
			return
		}
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(b))
		w.WriteHeader(204)
	}))
	defer remote.Close()
	a := NewAgent(remote.URL, nil, false)
	for i := 0; i < 3; i++ {
		a.Enqueue([]Sample{{Name: "x", Value: float64(i), TS: int64(i)}})
		if err := a.Flush(context.Background()); err == nil {
			t.Fatal("flush succeeded while down")
		}
	}
	mu.Lock()
	down = false
	mu.Unlock()
	if err := a.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Join(bodies, "") != "x 0 0\nx 1 1\nx 2 2\n" || a.queued != 0 || a.pushFails != 3 {
		t.Fatalf("%q queued=%d fails=%d", bodies, a.queued, a.pushFails)
	}
	// Beyond the buffer the oldest batches are dropped.
	a.Buffer = 2
	mu.Lock()
	down = true
	mu.Unlock()
	for i := 0; i < 4; i++ {
		a.Enqueue([]Sample{{Name: "y", Value: 1, TS: int64(i)}})
	}
	if a.queued != 2 || a.dropped != 2 {
		t.Fatalf("queued=%d dropped=%d", a.queued, a.dropped)
	}
}

func TestHostSamples(t *testing.T) {
	ss := HostSamples(1)
	names := map[string]bool{}
	for _, s := range ss {
		names[s.Name] = true
	}
	for _, n := range []string{"node_cpu_seconds_total", "node_memory_MemTotal_bytes"} {
		if !names[n] {
			t.Errorf("no %s (got %d samples)", n, len(ss))
		}
	}
}

func TestParseTargets(t *testing.T) {
	ts, err := parseTargets("a=http://x:1/metrics, b=http://y/m")
	if err != nil || len(ts) != 2 || ts[1].Job != "b" {
		t.Fatal(ts, err)
	}
	if _, err := parseTargets("nojob"); err == nil {
		t.Fatal("accepted")
	}
}
