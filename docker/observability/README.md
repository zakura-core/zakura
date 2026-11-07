# Zakura Observability Stack

Metrics and alerting for Zakura nodes.

## Quick Start

```bash
# Start the full observability stack
docker compose -f docker/docker-compose.observability.yml up -d

# View logs
docker compose -f docker/docker-compose.observability.yml logs -f zakura
```text

## Components

| Component | Port | URL | Purpose |
|-----------|------|-----|---------|
| **Zakura** | 9999, 8232 | - | Zcash node with metrics |
| **Prometheus** | 9094 | <http://localhost:9094> | Metrics collection and storage |
| **Grafana** | 3000 | <http://localhost:3000> | Dashboards and visualization |
| **AlertManager** | 9093 | <http://localhost:9093> | Alert routing |

Default Grafana credentials: `admin` / `admin` (you'll be prompted to change on first login)

## Architecture

```text
┌───────────────────┐
│   Zakura Node     │
│   :9999/metrics   │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│   Prometheus      │
│   :9094           │
│                   │
│   Scrapes metrics │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│     Grafana       │
│     :3000         │
│                   │
│  Dashboards for   │
│  metrics          │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│   AlertManager    │
│   :9093           │
│                   │
│  Routes alerts    │
└───────────────────┘
```text

## What Each Component Provides

### Metrics (Prometheus + Grafana)

Quantitative data about Zakura's behavior over time:

- **Network health**: Peer connections, bandwidth, message rates
- **Sync progress**: Block height, checkpoint verification, chain tip
- **Performance**: Block/transaction verification times
- **Resources**: Memory, connections, queue depths

See [grafana/README.md](grafana/README.md) for dashboard details.

### Alerts (AlertManager)

Automated notifications for operational issues:

- Critical: Negative value pools (ZIP-209 violation), release within 1 day of its end of service
- Warning: High RPC latency, sync stalls, peer connection issues, release within 3 days of its end of service

Configure alert destinations in [alertmanager/alertmanager.yml](alertmanager/alertmanager.yml).

## Configuration

### Environment Variables

Zakura accepts these environment variables for observability:

| Variable | Default | Description |
|----------|---------|-------------|
| `ZAKURA_METRICS__ENDPOINT_ADDR` | - | Prometheus metrics endpoint (e.g., `0.0.0.0:9999`) |
| `ZAKURA_RPC__LISTEN_ADDR` | - | RPC endpoint for JSON-RPC requests |

## Directory Structure

```text
observability/
├── README.md                    # This file
├── alertmanager/
│   └── alertmanager.yml         # Alert routing configuration
├── grafana/
│   ├── README.md                # Dashboard documentation
│   ├── dashboards/
│   │   ├── network_health.json  # Default home dashboard
│   │   └── *.json               # Other dashboards
│   └── provisioning/
│       ├── dashboards/
│       └── datasources/
└── prometheus/
    ├── prometheus.yaml          # Scrape configuration
    └── rules/                   # Recording and alert rules
```text

## Common Tasks

### View Zakura's current metrics

```bash
curl -s http://localhost:9999/metrics | grep zcash
```text

### Query Prometheus directly

```bash
# Current block height
curl -s 'http://localhost:9094/api/v1/query?query=zcash_state_tip_height'
```text

## Troubleshooting

### No metrics in Grafana

1. Check Zakura is exposing metrics: `curl http://localhost:9999/metrics`
2. Check Prometheus targets: <http://localhost:9094/targets>
3. Verify job name matches dashboard queries (default: `zakura`)

## Related Documentation

- [Grafana Dashboards](grafana/README.md) - Dashboard usage and configuration
- [Metrics Quick Start](../../docs/metrics-visualization-quickstart.md) - Adding new metrics
