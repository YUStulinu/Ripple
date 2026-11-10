// Command agent collects metrics and pushes them to Ripple.
//
//	agent -remote http://127.0.0.1:9201 -interval 15s -targets ripple=http://127.0.0.1:9201/metrics
//
// Every interval it
//   - reads this machine's CPU, memory, disk, filesystem and network counters (named like the
//     Prometheus node exporter's, with job="node");
//   - scrapes each target's Prometheus text endpoint, adding job and instance labels;
//   - adds its own metrics (scrape and push latency histograms, queue length);
//   - stamps everything with the collection time and POSTs it to Ripple's
//     /api/v1/import/prometheus.
//
// When Ripple is unreachable the batches stay queued (oldest dropped beyond -buffer samples) and
// are sent in order once it is back.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Sample is one line of the text format, stamped by the agent.
type Sample struct {
	Name   string
	Labels [][2]string
	Value  float64
	TS     int64 // ms
}

func escape(v string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(v)
}

func formatValue(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func (s Sample) String() string {
	var b strings.Builder
	b.WriteString(s.Name)
	if len(s.Labels) > 0 {
		b.WriteByte('{')
		for i, l := range s.Labels {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `%s="%s"`, l[0], escape(l[1]))
		}
		b.WriteByte('}')
	}
	fmt.Fprintf(&b, " %s %d", formatValue(s.Value), s.TS)
	return b.String()
}

// ParseLine parses one text-format line; ok is false for comments and blank lines.
func ParseLine(line string) (s Sample, ok bool, err error) {
	line = strings.TrimSpace(line)
	if line == "" || line[0] == '#' {
		return s, false, nil
	}
	i := 0
	for i < len(line) && (isNameChar(line[i]) || (i > 0 && line[i] >= '0' && line[i] <= '9')) {
		i++
	}
	if i == 0 {
		return s, false, fmt.Errorf("bad metric name in %q", line)
	}
	s.Name = line[:i]
	if i < len(line) && line[i] == '{' {
		i++
		for {
			for i < len(line) && (line[i] == ' ' || line[i] == ',') {
				i++
			}
			if i < len(line) && line[i] == '}' {
				i++
				break
			}
			st := i
			for i < len(line) && (isNameChar(line[i]) || (i > st && line[i] >= '0' && line[i] <= '9')) {
				i++
			}
			if i == st || i+1 >= len(line) || line[i] != '=' || line[i+1] != '"' {
				return s, false, fmt.Errorf("bad label in %q", line)
			}
			name := line[st:i]
			i += 2
			var v strings.Builder
			for {
				if i >= len(line) {
					return s, false, fmt.Errorf("unterminated value in %q", line)
				}
				c := line[i]
				if c == '"' {
					i++
					break
				}
				if c == '\\' && i+1 < len(line) {
					if line[i+1] == 'n' {
						v.WriteByte('\n')
					} else {
						v.WriteByte(line[i+1])
					}
					i += 2
					continue
				}
				v.WriteByte(c)
				i++
			}
			s.Labels = append(s.Labels, [2]string{name, v.String()})
		}
	}
	f := strings.Fields(line[i:])
	if len(f) == 0 {
		return s, false, fmt.Errorf("no value in %q", line)
	}
	s.Value, err = parseFloat(f[0])
	if err != nil {
		return s, false, fmt.Errorf("bad value in %q", line)
	}
	if len(f) > 1 {
		if s.TS, err = strconv.ParseInt(f[1], 10, 64); err != nil {
			return s, false, fmt.Errorf("bad timestamp in %q", line)
		}
	}
	return s, true, nil
}

func isNameChar(c byte) bool {
	return c == '_' || c == ':' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func parseFloat(s string) (float64, error) {
	switch s {
	case "+Inf", "Inf":
		s = "+Inf"
	case "NaN":
	}
	return strconv.ParseFloat(s, 64)
}

// WithLabels sets labels (replacing existing ones of the same name) and sorts them.
func (s Sample) WithLabels(extra ...[2]string) Sample {
	m := map[string]string{}
	for _, l := range s.Labels {
		m[l[0]] = l[1]
	}
	for _, l := range extra {
		m[l[0]] = l[1]
	}
	s.Labels = s.Labels[:0:0]
	for k, v := range m {
		s.Labels = append(s.Labels, [2]string{k, v})
	}
	sort.Slice(s.Labels, func(i, j int) bool { return s.Labels[i][0] < s.Labels[j][0] })
	return s
}

// Histogram is a cumulative histogram for the agent's own metrics.
type Histogram struct {
	Bounds []float64
	Counts []uint64
	Sum    float64
	Count  uint64
}

func NewHistogram() *Histogram {
	b := make([]float64, 16)
	for i := range b {
		b[i] = 0.0005 * float64(int(1)<<i)
	}
	return &Histogram{Bounds: b, Counts: make([]uint64, len(b))}
}

func (h *Histogram) Observe(v float64) {
	for i, b := range h.Bounds {
		if v <= b {
			h.Counts[i]++
		}
	}
	h.Sum += v
	h.Count++
}

func (h *Histogram) Samples(name string, labels [][2]string, ts int64) []Sample {
	var out []Sample
	for i, b := range h.Bounds {
		out = append(out, Sample{Name: name + "_bucket", Labels: append(append([][2]string{}, labels...), [2]string{"le", formatValue(b)}), Value: float64(h.Counts[i]), TS: ts})
	}
	out = append(out, Sample{Name: name + "_bucket", Labels: append(append([][2]string{}, labels...), [2]string{"le", "+Inf"}), Value: float64(h.Count), TS: ts})
	out = append(out, Sample{Name: name + "_sum", Labels: labels, Value: h.Sum, TS: ts}, Sample{Name: name + "_count", Labels: labels, Value: float64(h.Count), TS: ts})
	return out
}

type Target struct{ Job, URL string }

type Agent struct {
	Remote   string
	Targets  []Target
	Host     bool
	Instance string
	Buffer   int
	Client   *http.Client

	mu          sync.Mutex
	queue       [][]byte // pending batches, oldest first
	queued      int      // samples in queue
	dropped     int
	pushFails   int
	pushOK      int
	scrapeHist  map[string]*Histogram
	pushHist    *Histogram
	scrapeFails map[string]int
	hostSamples int
}

func NewAgent(remote string, targets []Target, host bool) *Agent {
	inst, _ := os.Hostname()
	return &Agent{
		Remote: remote, Targets: targets, Host: host, Instance: inst, Buffer: 2_000_000,
		Client:     &http.Client{Timeout: 10 * time.Second},
		scrapeHist: map[string]*Histogram{}, pushHist: NewHistogram(), scrapeFails: map[string]int{},
	}
}

// Scrape fetches one target and labels its samples.
func (a *Agent) Scrape(ctx context.Context, t Target, ts int64) ([]Sample, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", t.URL, nil)
	resp, err := a.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s: %s", t.URL, resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, err
	}
	u, _ := url.Parse(t.URL)
	var out []Sample
	for _, line := range strings.Split(string(body), "\n") {
		s, ok, err := ParseLine(line)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		s.TS = ts
		out = append(out, s.WithLabels([2]string{"job", t.Job}, [2]string{"instance", u.Host}))
	}
	return out, nil
}

// Collect gathers one round of samples from every source.
func (a *Agent) Collect(ctx context.Context, now time.Time) []Sample {
	ts := now.UnixMilli()
	var out []Sample
	if a.Host {
		hs := HostSamples(ts)
		for i := range hs {
			hs[i] = hs[i].WithLabels([2]string{"job", "node"}, [2]string{"instance", a.Instance})
		}
		out = append(out, hs...)
		a.hostSamples = len(hs)
	}
	for _, t := range a.Targets {
		t0 := time.Now()
		ss, err := a.Scrape(ctx, t, ts)
		a.mu.Lock()
		h := a.scrapeHist[t.Job]
		if h == nil {
			h = NewHistogram()
			a.scrapeHist[t.Job] = h
		}
		h.Observe(time.Since(t0).Seconds())
		if err != nil {
			a.scrapeFails[t.Job]++
			log.Printf("scrape %s: %v", t.Job, err)
		}
		a.mu.Unlock()
		up := 1.0
		if err != nil {
			up = 0
		}
		out = append(out, Sample{Name: "up", Labels: [][2]string{{"job", t.Job}, {"instance", t.URL}}, Value: up, TS: ts})
		out = append(out, ss...)
	}
	out = append(out, a.selfSamples(ts)...)
	return out
}

func (a *Agent) selfSamples(ts int64) []Sample {
	a.mu.Lock()
	defer a.mu.Unlock()
	base := [][2]string{{"job", "agent"}, {"instance", a.Instance}}
	with := func(extra ...[2]string) [][2]string { return append(append([][2]string{}, base...), extra...) }
	out := []Sample{
		{Name: "ripple_agent_queue_samples", Labels: base, Value: float64(a.queued), TS: ts},
		{Name: "ripple_agent_dropped_samples_total", Labels: base, Value: float64(a.dropped), TS: ts},
		{Name: "ripple_agent_push_failures_total", Labels: base, Value: float64(a.pushFails), TS: ts},
		{Name: "ripple_agent_pushes_total", Labels: base, Value: float64(a.pushOK), TS: ts},
		{Name: "ripple_agent_host_samples", Labels: base, Value: float64(a.hostSamples), TS: ts},
	}
	out = append(out, a.pushHist.Samples("ripple_agent_push_duration_seconds", base, ts)...)
	jobs := make([]string, 0, len(a.scrapeHist))
	for j := range a.scrapeHist {
		jobs = append(jobs, j)
	}
	sort.Strings(jobs)
	for _, j := range jobs {
		out = append(out, a.scrapeHist[j].Samples("ripple_agent_scrape_duration_seconds", with([2]string{"target", j}), ts)...)
		out = append(out, Sample{Name: "ripple_agent_scrape_failures_total", Labels: with([2]string{"target", j}), Value: float64(a.scrapeFails[j]), TS: ts})
	}
	return out
}

// Enqueue adds a batch; beyond Buffer samples the oldest batches are dropped.
func (a *Agent) Enqueue(ss []Sample) {
	if len(ss) == 0 {
		return
	}
	var b bytes.Buffer
	for _, s := range ss {
		b.WriteString(s.String())
		b.WriteByte('\n')
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.queue = append(a.queue, b.Bytes())
	a.queued += len(ss)
	for a.queued > a.Buffer && len(a.queue) > 1 {
		n := bytes.Count(a.queue[0], []byte{'\n'})
		a.queue = a.queue[1:]
		a.queued -= n
		a.dropped += n
	}
}

// Flush sends queued batches in order and stops at the first failure (they are retried later).
func (a *Agent) Flush(ctx context.Context) error {
	for {
		a.mu.Lock()
		if len(a.queue) == 0 {
			a.mu.Unlock()
			return nil
		}
		batch := a.queue[0]
		a.mu.Unlock()
		t0 := time.Now()
		req, _ := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(a.Remote, "/")+"/api/v1/import/prometheus", bytes.NewReader(batch))
		req.Header.Set("Content-Type", "text/plain")
		resp, err := a.Client.Do(req)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode/100 != 2 {
				err = fmt.Errorf("remote: %s", resp.Status)
			}
		}
		a.mu.Lock()
		a.pushHist.Observe(time.Since(t0).Seconds())
		if err != nil {
			a.pushFails++
			a.mu.Unlock()
			return err
		}
		a.pushOK++
		a.queue = a.queue[1:]
		a.queued -= bytes.Count(batch, []byte{'\n'})
		a.mu.Unlock()
	}
}

// Metrics serves the agent's own metrics (also pushed to Ripple).
func (a *Agent) Metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	for _, s := range a.selfSamples(time.Now().UnixMilli()) {
		s.TS = 0
		line := s.String()
		fmt.Fprintln(w, line[:strings.LastIndexByte(line, ' ')])
	}
}

func parseTargets(s string) ([]Target, error) {
	var out []Target
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p == "" {
			continue
		}
		job, u, ok := strings.Cut(p, "=")
		if !ok || job == "" {
			return nil, fmt.Errorf("target %q: use job=url", p)
		}
		if _, err := url.ParseRequestURI(u); err != nil {
			return nil, fmt.Errorf("target %q: %v", p, err)
		}
		out = append(out, Target{Job: job, URL: u})
	}
	return out, nil
}

func main() {
	remote := flag.String("remote", "http://127.0.0.1:9201", "Ripple address")
	interval := flag.Duration("interval", 15*time.Second, "collection interval")
	targets := flag.String("targets", "", "job=url,... Prometheus endpoints to scrape")
	host := flag.Bool("host", true, "collect this machine's metrics")
	listen := flag.String("listen", "127.0.0.1:9202", "address for the agent's own /metrics ('' to disable)")
	buffer := flag.Int("buffer", 2_000_000, "samples kept while Ripple is unreachable")
	flag.Parse()
	ts, err := parseTargets(*targets)
	if err != nil {
		log.Fatal(err)
	}
	a := NewAgent(*remote, ts, *host)
	a.Buffer = *buffer
	if *listen != "" {
		go func() { log.Println(http.ListenAndServe(*listen, http.HandlerFunc(a.Metrics))) }()
	}
	log.Printf("collecting every %s, pushing to %s", *interval, *remote)
	tick := time.NewTicker(*interval)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), *interval)
		a.Enqueue(a.Collect(ctx, time.Now()))
		if err := a.Flush(ctx); err != nil {
			log.Printf("push: %v (%d samples queued)", err, a.queued)
		}
		cancel()
		<-tick.C
	}
}
