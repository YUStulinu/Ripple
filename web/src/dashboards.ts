import type { Unit } from "./format";

export interface Panel {
  title: string;
  queries: string[]; // $__rate_interval becomes a window suited to the step
  unit: Unit;
}

export interface Dashboard {
  id: string;
  title: string;
  panels: Panel[];
}

const CORES = 'scalar(count(count by (cpu) (node_cpu_seconds_total{mode="idle"})))';

export const DASHBOARDS: Dashboard[] = [
  {
    id: "host",
    title: "This computer",
    panels: [
      { title: "CPU busy by mode", unit: "percent", queries: [`100 * sum by (mode) (rate(node_cpu_seconds_total{mode!="idle"}[$__rate_interval])) / ${CORES}`] },
      { title: "Memory used", unit: "percent", queries: ["100 * node_memory_used_bytes / node_memory_MemTotal_bytes"] },
      { title: "Network received", unit: "bytes/s", queries: ["sum by (device) (rate(node_network_receive_bytes_total[$__rate_interval]))"] },
      { title: "Network sent", unit: "bytes/s", queries: ["sum by (device) (rate(node_network_transmit_bytes_total[$__rate_interval]))"] },
      { title: "Disk reads", unit: "bytes/s", queries: ["sum by (device) (rate(node_disk_read_bytes_total[$__rate_interval]))"] },
      { title: "Disk writes", unit: "bytes/s", queries: ["sum by (device) (rate(node_disk_written_bytes_total[$__rate_interval]))"] },
      { title: "Filesystem free", unit: "percent", queries: ["100 * node_filesystem_avail_bytes / node_filesystem_size_bytes"] },
      { title: "Processes", unit: "none", queries: ["node_processes"] },
    ],
  },
  {
    id: "ripple",
    title: "Ripple",
    panels: [
      { title: "Samples written", unit: "ops", queries: ["sum(rate(ripple_samples_appended_total[$__rate_interval]))"] },
      { title: "Series in the head", unit: "none", queries: ["sum(ripple_head_series)"] },
      {
        title: "WAL fsync latency (p50, p99)",
        unit: "seconds",
        queries: [
          "histogram_quantile(0.5, sum by (le) (rate(ripple_wal_fsync_duration_seconds_bucket[$__rate_interval])))",
          "histogram_quantile(0.99, sum by (le) (rate(ripple_wal_fsync_duration_seconds_bucket[$__rate_interval])))",
        ],
      },
      {
        title: "HTTP latency p99 by handler",
        unit: "seconds",
        queries: ["histogram_quantile(0.99, sum by (le, handler) (rate(ripple_http_request_duration_seconds_bucket[$__rate_interval])))"],
      },
      { title: "Storage", unit: "bytes", queries: ["sum(ripple_block_bytes)", "sum(ripple_wal_bytes)", "sum(ripple_head_chunk_bytes)"] },
      { title: "Bytes per sample in blocks", unit: "none", queries: ["sum(ripple_block_bytes) / sum(ripple_block_samples)"] },
      {
        title: "Agent push latency (p99)",
        unit: "seconds",
        queries: ["histogram_quantile(0.99, sum by (le) (rate(ripple_agent_push_duration_seconds_bucket[$__rate_interval])))"],
      },
      { title: "Agent queue", unit: "none", queries: ["sum(ripple_agent_queue_samples)"] },
    ],
  },
];
