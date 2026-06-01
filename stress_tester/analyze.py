#!/usr/bin/env python3
"""qRPC Benchmark Analysis Tool

Usage:
  python3 analyze.py <results_dir> [--html report.html]

Scans JSON result files, prints a summary table and generates
an HTML report with interactive latency / RPS charts.
"""

import json
import os
import sys
from collections import defaultdict
from pathlib import Path

REPORT_FIELDS = {
    "system":         ("System",     "%s"),
    "profile":        ("Profile",    "%s"),
    "total_requests": ("Total",      "%d"),
    "success_count":  ("OK",         "%d"),
    "error_count":    ("Errors",     "%d"),
    "success_rate":   ("OK%%",       "%.2f"),
    "rps":            ("RPS",        "%.1f"),
    "avg_latency":    ("Avg",        "lat_ns"),
    "min_latency":    ("Min",        "lat_ns"),
    "max_latency":    ("Max",        "lat_ns"),
    "p50":            ("P50",        "lat_ns"),
    "p90":            ("P90",        "lat_ns"),
    "p95":            ("P95",        "lat_ns"),
    "p99":            ("P99",        "lat_ns"),
    "p99_9":          ("P99.9",      "lat_ns"),
}

PALLETE = [
    "#4e79a7", "#f28e2b", "#e15759", "#76b7b2",
    "#59a14f", "#edc948", "#b07aa1", "#ff9da7",
    "#9c755f", "#bab0ac",
]

def load_results(directory: str) -> list[dict]:
    results = []
    for f in sorted(Path(directory).glob("bench_*.json")):
        try:
            data = json.loads(f.read_text())
            results.append(data)
        except (json.JSONDecodeError, OSError) as e:
            print(f"[warn] skip {f.name}: {e}", file=sys.stderr)
    return results


def fmt_latency(ns: int) -> str:
    if ns < 1_000:
        return f"{ns}ns"
    if ns < 1_000_000:
        return f"{ns/1_000:.1f}µs"
    if ns < 1_000_000_000:
        return f"{ns/1_000_000:.2f}ms"
    return f"{ns/1_000_000_000:.3f}s"


def fmt_rps(v: float) -> str:
    if v >= 1_000_000:
        return f"{v/1_000_000:.2f}M"
    if v >= 1_000:
        return f"{v/1_000:.2f}K"
    return f"{v:.1f}"


def latency_col(ns: int, width: int = 12) -> str:
    return fmt_latency(ns).rjust(width)


def print_table(results: list[dict]):
    if not results:
        return

    cols = ["scenario", "profile", "system", "total", "success_rate", "rps",
            "avg_latency", "p50", "p95", "p99", "p99_9", "max_latency"]
    headers = ["Scenario", "Profile", "System", "Total", "OK%", "RPS",
               "Avg", "P50", "P95", "P99", "P99.9", "Max"]

    rows = []
    for r in results:
        rows.append([
            r.get("scenario", "?"),
            r.get("profile", "?"),
            r.get("system", "?"),
            str(r.get("total_requests", 0)),
            f'{r.get("success_rate", 0):.1f}',
            fmt_rps(r.get("rps", 0)),
            fmt_latency(r.get("avg_latency", 0)),
            fmt_latency(r.get("p50", 0)),
            fmt_latency(r.get("p95", 0)),
            fmt_latency(r.get("p99", 0)),
            fmt_latency(r.get("p99_9", 0)),
            fmt_latency(r.get("max_latency", 0)),
        ])

    col_widths = [max(len(str(r[i])) for r in rows + [headers]) for i in range(len(headers))]
    sep = "+-" + "-+-".join("-" * w for w in col_widths) + "-+"
    fmt_row = "| " + " | ".join("%%-%ds" % w for w in col_widths) + " |"

    print(sep)
    print(fmt_row % tuple(headers))
    print(sep)
    for row in rows:
        print(fmt_row % tuple(row))
    print(sep)


def group_by(results: list[dict], key: str) -> dict[str, list[dict]]:
    g = defaultdict(list)
    for r in results:
        g[r.get(key, "?")].append(r)
    return dict(g)


def generate_html(results: list[dict], output_path: str):
    by_scenario = group_by(results, "scenario")
    systems = sorted(set(r.get("system", "?") for r in results))

    html = """<!DOCTYPE html><html><head><meta charset="utf-8">
<title>qRPC Benchmark Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         margin: 20px; background: #f8f9fa; color: #222; }
  h1 { border-bottom: 2px solid #4e79a7; padding-bottom: 8px; }
  h2 { color: #4e79a7; margin-top: 32px; }
  table { border-collapse: collapse; margin: 12px 0 24px; font-size: 14px; }
  th, td { border: 1px solid #ccc; padding: 6px 12px; text-align: right; }
  th { background: #4e79a7; color: #fff; font-weight: 600; }
  td:first-child { text-align: left; font-weight: 500; }
  tr:nth-child(even) { background: #f1f3f5; }
  .chart-container { width: 800px; max-width: 100%; margin: 20px 0; }
  .chart-row { display: flex; flex-wrap: wrap; gap: 24px; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 4px;
           font-size: 12px; font-weight: 600; }
  .badge-qrpc { background: #4e79a7; color: #fff; }
  .badge-grpc { background: #e15759; color: #fff; }
</style></head><body>
<h1>📊 qRPC Benchmark Report</h1>
<p>Generated: """ + __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S") + """</p>
"""

    for scenario, reports in by_scenario.items():
        html += f"<h2>{scenario.replace('_', ' ').title()}</h2>\n"
        html += _table_html(reports)
        html += _rps_chart_html(scenario, reports, systems)
        html += _latency_chart_html(scenario, reports, systems)
        html += _tail_chart_html(scenario, reports, systems)

    html += "</body></html>"
    Path(output_path).write_text(html)
    print(f"[ok] HTML report: {output_path}", file=sys.stderr)


def _table_html(reports: list[dict]) -> str:
    cols = ["profile", "system", "total_requests", "success_rate", "rps",
            "avg_latency", "p50", "p95", "p99", "p99_9", "max_latency"]
    labels = ["Profile", "System", "Total", "OK%", "RPS",
              "Avg", "P50", "P95", "P99", "P99.9", "Max"]

    h = '<table><thead><tr>' + ''.join(f'<th>{c}</th>' for c in labels) + '</tr></thead><tbody>'
    for r in reports:
        s = r.get("system", "")
        badge = f'<span class="badge badge-{s}">{s}</span>' if s else ""
        vals = [
            r.get("profile", ""),
            badge,
            f'{r.get("total_requests", 0):,}',
            f'{r.get("success_rate", 0):.1f}',
            fmt_rps(r.get("rps", 0)),
            fmt_latency(r.get("avg_latency", 0)),
            fmt_latency(r.get("p50", 0)),
            fmt_latency(r.get("p95", 0)),
            fmt_latency(r.get("p99", 0)),
            fmt_latency(r.get("p99_9", 0)),
            fmt_latency(r.get("max_latency", 0)),
        ]
        h += '<tr>' + ''.join(f'<td>{v}</td>' for v in vals) + '</tr>'
    return h + '</tbody></table>'


def _profile_order():
    return ["clean", "wifi", "lte", "bad_lte", "extreme"]


def _chart_data(reports: list[dict], systems: list[str],
                value_fn, label: str) -> tuple:
    profiles = _profile_order()
    datasets = []
    used_colors = {}
    for i, sys_name in enumerate(systems):
        color = PALLETE[i % len(PALLETE)]
        used_colors[sys_name] = color
        data = []
        for p in profiles:
            vals = [value_fn(r) for r in reports
                    if r.get("profile") == p and r.get("system") == sys_name]
            data.append(vals[0] if vals else None)
        datasets.append({
            "label": sys_name,
            "data": data,
            "backgroundColor": color,
            "borderColor": color,
            "borderWidth": 2,
            "fill": False,
        })
    return profiles, datasets


def _rps_chart_html(scenario: str, reports: list[dict], systems: list[str]) -> str:
    profiles, datasets = _chart_data(reports, systems,
        lambda r: round(r.get("rps", 0), 1), "RPS")
    return _chart_script(scenario, "rps", "bar", "RPS by Profile", profiles, datasets)


def _latency_chart_html(scenario: str, reports: list[dict], systems: list[str]) -> str:
    profiles, datasets = _chart_data(reports, systems,
        lambda r: round(r.get("avg_latency", 0) / 1_000_000, 2), "Avg Latency (ms)")
    return _chart_script(scenario, "latency", "bar",
        "Average Latency by Profile (ms)", profiles, datasets)


def _tail_chart_html(scenario: str, reports: list[dict], systems: list[str]) -> str:
    profiles, p95_datasets = _chart_data(reports, systems,
        lambda r: round(r.get("p95", 0) / 1_000_000, 2), "P95 (ms)")
    p99_label = "P99 (ms)"
    _, p99_datasets = _chart_data(reports, systems,
        lambda r: round(r.get("p99", 0) / 1_000_000, 2), p99_label)
    combined = []
    for ds95, ds99 in zip(p95_datasets, p99_datasets):
        combined.append(ds95)
        d = dict(ds99)
        d["label"] = d["label"] + " P99"
        d["borderDash"] = [5, 5]
        combined.append(d)
    return _chart_script(scenario, "tail", "line",
        "Tail Latency: P95 / P99 by Profile (ms)", profiles, combined)


def _chart_script(scenario: str, suffix: str, kind: str,
                  title: str, labels: list[str], datasets: list[dict]) -> str:
    safe = scenario.replace("_", "") + suffix
    return f"""
<div class="chart-container"><canvas id="{safe}"></canvas></div>
<script>
new Chart(document.getElementById("{safe}"), {{
  type: "{kind}",
  data: {{ labels: {json.dumps(labels)}, datasets: {json.dumps(datasets)} }},
  options: {{
    responsive: true,
    plugins: {{ legend: {{ position: "top" }}, title: {{ display: true, text: "{title}" }} }},
    scales: {{
      y: {{ beginAtZero: true, title: {{ display: true, text: "ms" if "ms" in title else "" }} }},
      x: {{ title: {{ display: true, text: "Network Profile" }} }}
    }}
  }}
}});
</script>"""


def main():
    args = sys.argv[1:]
    directory = args[0] if args else "results"
    html_output = None
    if "--html" in args:
        idx = args.index("--html")
        html_output = args[idx + 1] if idx + 1 < len(args) else "report.html"

    if not os.path.isdir(directory):
        print(f"Usage: {sys.argv[0]} <results_dir> [--html report.html]", file=sys.stderr)
        sys.exit(1)

    results = load_results(directory)
    if not results:
        print(f"No benchmark results found in {directory}/", file=sys.stderr)
        sys.exit(1)

    print(f"\n  Loaded {len(results)} result(s) from {directory}/\n")
    print_table(results)

    if html_output:
        generate_html(results, html_output)


if __name__ == "__main__":
    main()
