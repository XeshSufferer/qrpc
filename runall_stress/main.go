package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var profiles = []string{"clean", "wifi", "lte", "bad_lte", "extreme"}
var payloadSizes = []string{"100", "4000"}
var connectionCounts = []int{1, 16}
var scenarios = []string{
	"baseline_latency",
	"concurrency_stress",
	"multiplex_stress",
	"loss_sensitivity",
	"rtt_scaling",
}

type Report struct {
	Scenario      string         `json:"scenario"`
	Profile       string         `json:"profile"`
	System        string         `json:"system"`
	SuccessCount  int64          `json:"success_count"`
	TotalRequests int64          `json:"total_requests"`
	SuccessRate   float64        `json:"success_rate"`
	RPS           float64        `json:"rps"`
	AvgLatency    int64          `json:"avg_latency"`
	MinLatency    int64          `json:"min_latency"`
	MaxLatency    int64          `json:"max_latency"`
	P50           int64          `json:"p50"`
	P90           int64          `json:"p90"`
	P95           int64          `json:"p95"`
	P99           int64          `json:"p99"`
	P999          int64          `json:"p99_9"`
	LatencyDist   map[string]int `json:"latency_distribution,omitempty"`
	Elapsed       int64          `json:"elapsed"`
	LoadConfig    map[string]any `json:"load_config"`
}

type Cell struct {
	Payload string
	Profile string
	RPS     float64
	P95     int64
	P50     int64
	P99     int64
	Avg     int64
	Success float64
	Total   int64
	HasData bool
}

type ScenarioData struct {
	Name   string
	Cells  []Cell
	MaxRPS float64
	MaxP95 int64
}

func findRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			log.Fatalf("project root (go.mod) not found from %v", dir)
		}
		dir = parent
	}
}

func main() {
	start := time.Now()
	root := findRoot()

	benchBin := filepath.Join(root, "stress_tester", "stress_tester")
	resultsDir := "results"
	os.MkdirAll(resultsDir, 0755)

	log.Println("[build] compiling stress_tester binary ...")
	cmd := exec.Command("go", "build", "-o", benchBin, ".")
	cmd.Dir = filepath.Join(root, "stress_tester")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("build failed: %v", err)
	}

	log.Println("[server] starting benchmark server ...")
	serverCmd := exec.Command("sudo", benchBin, "server", "-addr", "127.0.0.1:8181")
	serverCmd.Stderr = os.Stderr
	if err := serverCmd.Start(); err != nil {
		log.Fatalf("server start: %v", err)
	}
	time.Sleep(2 * time.Second)

	serverCleanup := func() {
		serverCmd.Process.Kill()
		serverCmd.Wait()
	}
	defer serverCleanup()
	defer func() {
		exec.Command("sudo", "tc", "qdisc", "del", "dev", "lo", "root").Run()
	}()

	total := len(scenarios) * len(profiles) * len(payloadSizes) * len(connectionCounts)
	done := 0

	for _, conn := range connectionCounts {
		for _, sc := range scenarios {
			for _, prof := range profiles {
				for _, pay := range payloadSizes {
					done++
					log.Printf("[%d/%d] scenario=%s profile=%s payload=%sB conns=%d",
						done, total, sc, prof, pay, conn)

					cmd := exec.Command("sudo", benchBin, "run",
						"-scenario", sc,
						"-profile", prof,
						"-payload-size", pay,
						"-streams", "16",
						"-connections", strconv.Itoa(conn),
						"-pipelining", "64",
						"-duration", "10s",
						"-warmup", "3s",
						"-addr", "127.0.0.1:8181",
						"-output", resultsDir,
					)
					cmd.Stdout = os.Stdout
					cmd.Stderr = os.Stderr
					if err := cmd.Run(); err != nil {
						log.Printf("  [warn] run failed: %v", err)
					}
				}
			}
		}
	}

	log.Println("[report] generating heatmap report ...")
	generateReport(resultsDir)

	log.Printf("[done] total time: %v", time.Since(start).Round(time.Second))
}

func loadResults(dir string) []Report {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var reports []Report
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "bench_") || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		var r Report
		if err := json.Unmarshal(data, &r); err != nil {
			continue
		}
		reports = append(reports, r)
	}
	return reports
}

func extractPayload(filename string) string {
	p := strings.TrimSuffix(filename, ".json")
	parts := strings.Split(p, "_pay")
	if len(parts) == 2 {
		return parts[1]
	}
	return "?"
}

func generateReport(dir string) {
	reports := loadResults(dir)
	if len(reports) == 0 {
		log.Println("  no results found")
		return
	}

	grouped := make(map[string]map[string]map[string]Report)
	for _, r := range reports {
		if grouped[r.Scenario] == nil {
			grouped[r.Scenario] = make(map[string]map[string]Report)
		}
		if grouped[r.Scenario][r.Profile] == nil {
			grouped[r.Scenario][r.Profile] = make(map[string]Report)
		}
		pay := fmt.Sprintf("%d", int(r.LoadConfig["payload_size"].(float64)))
		grouped[r.Scenario][r.Profile][pay] = r
	}

	var allScenarios []ScenarioData
	for _, sc := range scenarios {
		sd := ScenarioData{Name: sc}
		for _, prof := range profiles {
			for _, pay := range payloadSizes {
				c := Cell{
					Payload: pay,
					Profile: prof,
				}
				if r, ok := grouped[sc][prof][pay]; ok {
					c.HasData = true
					c.RPS = r.RPS
					c.P95 = r.P95
					c.P50 = r.P50
					c.P99 = r.P99
					c.Avg = r.AvgLatency
					c.Success = r.SuccessRate
					c.Total = r.TotalRequests
					if r.RPS > sd.MaxRPS {
						sd.MaxRPS = r.RPS
					}
					if r.P95 > sd.MaxP95 {
						sd.MaxP95 = r.P95
					}
				}
				sd.Cells = append(sd.Cells, c)
			}
		}
		allScenarios = append(allScenarios, sd)
	}

	htmlPath := filepath.Join(dir, "heatmap.html")
	f, err := os.Create(htmlPath)
	if err != nil {
		log.Fatalf("create html: %v", err)
	}
	defer f.Close()

	fmt.Fprint(f, htmlHeader)

	for _, sd := range allScenarios {
		renderScenario(f, sd)
	}

	fmt.Fprint(f, summaryTable(allScenarios))
	fmt.Fprint(f, htmlFooter)

	log.Printf("  report: %s", htmlPath)

	printConsoleSummary(allScenarios)
}

func renderScenario(f *os.File, sd ScenarioData) {
	fmt.Fprintf(f, `<div class="scenario">
<h2>%s</h2>
<table>
<thead><tr><th>Payload</th>`, sd.Name)
	for _, prof := range profiles {
		fmt.Fprintf(f, `<th class="rotated"><span>%s</span></th>`, prof)
	}
	fmt.Fprint(f, `</tr></thead><tbody>`)

	for _, pay := range payloadSizes {
		fmt.Fprintf(f, `<tr><td class="pay">%s B</td>`, pay)
		for _, prof := range profiles {
			var c Cell
			for _, cell := range sd.Cells {
				if cell.Payload == pay && cell.Profile == prof {
					c = cell
					break
				}
			}
			if !c.HasData {
				fmt.Fprint(f, `<td class="no-data">—</td>`)
				continue
			}
			rpsBg := heatColor(c.RPS, 0, sd.MaxRPS, false)
			p95Bg := heatColor(float64(c.P95), 0, float64(sd.MaxP95), true)
			fmt.Fprintf(f, `<td class="data-cell">
				<div class="rps" style="background:%s">%s</div>
				<div class="p95" style="background:%s">P95 %s</div>
			</td>`,
				rpsBg, fmtRPS(c.RPS),
				p95Bg, fmtLatency(c.P95))
		}
		fmt.Fprint(f, `</tr>`)
	}
	fmt.Fprint(f, `</tbody></table></div>`)
}

func summaryTable(scenarios []ScenarioData) string {
	var b strings.Builder
	b.WriteString(`<div class="scenario"><h2>Summary — RPS</h2><table>
<thead><tr><th>Scenario</th>`)
	for _, prof := range profiles {
		fmt.Fprintf(&b, `<th>%s</th>`, prof)
	}
	b.WriteString(`</tr></thead><tbody>`)

	for _, sd := range scenarios {
		fmt.Fprintf(&b, `<tr><td>%s</td>`, sd.Name)
		for _, prof := range profiles {
			best := 0.0
			for _, c := range sd.Cells {
				if c.Profile == prof && c.HasData && c.RPS > best {
					best = c.RPS
				}
			}
			if best > 0 {
				fmt.Fprintf(&b, `<td>%s</td>`, fmtRPS(best))
			} else {
				fmt.Fprint(&b, `<td>—</td>`)
			}
		}
		b.WriteString(`</tr>`)
	}
	b.WriteString(`</tbody></table></div>`)

	b.WriteString(`<div class="scenario"><h2>Summary — P95 Latency</h2><table>
<thead><tr><th>Scenario</th>`)
	for _, prof := range profiles {
		fmt.Fprintf(&b, `<th>%s</th>`, prof)
	}
	b.WriteString(`</tr></thead><tbody>`)

	for _, sd := range scenarios {
		fmt.Fprintf(&b, `<tr><td>%s</td>`, sd.Name)
		for _, prof := range profiles {
			worst := int64(0)
			for _, c := range sd.Cells {
				if c.Profile == prof && c.HasData && c.P95 > worst {
					worst = c.P95
				}
			}
			if worst > 0 {
				fmt.Fprintf(&b, `<td>%s</td>`, fmtLatency(worst))
			} else {
				fmt.Fprint(&b, `<td>—</td>`)
			}
		}
		b.WriteString(`</tr>`)
	}
	b.WriteString(`</tbody></table></div>`)
	return b.String()
}

func printConsoleSummary(scenarios []ScenarioData) {
	fmt.Println("\n" + strings.Repeat("=", 90))
	fmt.Println("RESULTS SUMMARY (best RPS across payload sizes)")
	fmt.Println(strings.Repeat("=", 90))

	header := fmt.Sprintf("%-22s", "Scenario")
	for _, p := range profiles {
		header += fmt.Sprintf(" %12s", p)
	}
	fmt.Println(header)
	fmt.Println(strings.Repeat("-", 90))

	for _, sd := range scenarios {
		line := fmt.Sprintf("%-22s", sd.Name)
		for _, prof := range profiles {
			best := 0.0
			for _, c := range sd.Cells {
				if c.Profile == prof && c.HasData && c.RPS > best {
					best = c.RPS
				}
			}
			if best > 0 {
				line += fmt.Sprintf(" %12s", fmtRPS(best))
			} else {
				line += fmt.Sprintf(" %12s", "—")
			}
		}
		fmt.Println(line)
	}

	fmt.Println(strings.Repeat("=", 90))
	fmt.Println("\nTAIL LATENCY (worst P95 across payload sizes)")
	fmt.Println(strings.Repeat("-", 90))
	header2 := fmt.Sprintf("%-22s", "Scenario")
	for _, p := range profiles {
		header2 += fmt.Sprintf(" %12s", p)
	}
	fmt.Println(header2)
	fmt.Println(strings.Repeat("-", 90))

	for _, sd := range scenarios {
		line := fmt.Sprintf("%-22s", sd.Name)
		for _, prof := range profiles {
			worst := int64(0)
			for _, c := range sd.Cells {
				if c.Profile == prof && c.HasData && c.P95 > worst {
					worst = c.P95
				}
			}
			if worst > 0 {
				line += fmt.Sprintf(" %12s", fmtLatency(worst))
			} else {
				line += fmt.Sprintf(" %12s", "—")
			}
		}
		fmt.Println(line)
	}
	fmt.Println(strings.Repeat("-", 90))
}

func heatColor(val, min, max float64, reverse bool) string {
	if max == min {
		return rgba(128, 255, 128, 30)
	}
	ratio := (val - min) / (max - min)
	if ratio > 1 {
		ratio = 1
	}
	if reverse {
		ratio = 1 - ratio
	}
	r := int(255 * (1 - ratio))
	g := int(255 * ratio)
	return rgba(r, g, 80, 40+int(60*ratio))
}

func rgba(r, g, b, a int) string {
	if r > 255 {
		r = 255
	}
	if g > 255 {
		g = 255
	}
	if b > 255 {
		b = 255
	}
	if a > 100 {
		a = 100
	}
	if a < 0 {
		a = 0
	}
	return fmt.Sprintf("rgba(%d,%d,%d,%.2f)", r, g, b, float64(a)/100)
}

func fmtLatency(ns int64) string {
	switch {
	case ns >= 1_000_000_000:
		return fmt.Sprintf("%.2fs", float64(ns)/1e9)
	case ns >= 1_000_000:
		return fmt.Sprintf("%.1fms", float64(ns)/1e6)
	case ns >= 1_000:
		return fmt.Sprintf("%.1fµs", float64(ns)/1e3)
	default:
		return fmt.Sprintf("%dns", ns)
	}
}

func fmtRPS(v float64) string {
	switch {
	case v >= 1_000_000:
		return fmt.Sprintf("%.2fM", v/1e6)
	case v >= 1_000:
		return fmt.Sprintf("%.1fK", v/1e3)
	default:
		return fmt.Sprintf("%.0f", v)
	}
}

const htmlHeader = `<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8">
<title>qRPC Benchmark Heatmap</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         margin: 30px; background: #f8f9fa; color: #222; }
  h1 { border-bottom: 2px solid #4e79a7; padding-bottom: 8px; }
  h2 { color: #4e79a7; margin: 28px 0 12px; clear: both; }
  .scenario { margin-bottom: 40px; }
  table { border-collapse: collapse; font-size: 13px; }
  th, td { border: 1px solid #ccc; padding: 8px 12px; text-align: center; }
  th { background: #4e79a7; color: #fff; font-weight: 600; }
  th.rotated { height: 80px; white-space: nowrap; vertical-align: bottom; }
  th.rotated span { writing-mode: vertical-lr; transform: rotate(180deg); }
  td.pay { font-weight: 600; text-align: right; background: #e9ecef; }
  td.no-data { color: #aaa; font-style: italic; }
  .data-cell { padding: 0; }
  .data-cell div { padding: 4px 8px; }
  .rps { font-weight: 700; font-size: 14px; border-bottom: 1px solid rgba(0,0,0,0.1); }
  .p95 { font-size: 12px; color: #444; }
  tr:nth-child(even) td.pay { background: #dee2e6; }
  .summary-grid { display: flex; flex-wrap: wrap; gap: 20px; }
  .summary-grid table { flex: 0 0 auto; }
</style></head><body>
<h1>📊 qRPC Benchmark — Heatmap</h1>
<p>Payload sizes: 100 B / 4 KB &nbsp;|&nbsp; Connections: 1 / 16 &nbsp;|&nbsp; Pipelining: 64 &nbsp;|&nbsp; Duration: 10s + 3s warmup</p>
`

const htmlFooter = `</body></html>`
