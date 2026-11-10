package main

import (
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/net"
	"github.com/shirou/gopsutil/v4/process"
	"strconv"
	"strings"
)

// HostSamples reads this machine's counters, named like the Prometheus node exporter's.
// Sources that fail on this platform are skipped.
func HostSamples(ts int64) []Sample {
	var out []Sample
	add := func(name string, v float64, labels ...[2]string) {
		out = append(out, Sample{Name: name, Labels: labels, Value: v, TS: ts})
	}
	if times, err := cpu.Times(true); err == nil {
		for i, t := range times {
			c := [2]string{"cpu", strconv.Itoa(i)}
			for _, m := range []struct {
				mode string
				v    float64
			}{{"user", t.User}, {"system", t.System}, {"idle", t.Idle}, {"iowait", t.Iowait}, {"irq", t.Irq}, {"softirq", t.Softirq}, {"nice", t.Nice}, {"steal", t.Steal}} {
				if m.v > 0 || m.mode == "user" || m.mode == "system" || m.mode == "idle" {
					add("node_cpu_seconds_total", m.v, c, [2]string{"mode", m.mode})
				}
			}
		}
	}
	if vm, err := mem.VirtualMemory(); err == nil {
		add("node_memory_MemTotal_bytes", float64(vm.Total))
		add("node_memory_MemAvailable_bytes", float64(vm.Available))
		add("node_memory_used_bytes", float64(vm.Used))
	}
	if sw, err := mem.SwapMemory(); err == nil {
		add("node_memory_SwapTotal_bytes", float64(sw.Total))
		add("node_memory_SwapUsed_bytes", float64(sw.Used))
	}
	if io, err := disk.IOCounters(); err == nil {
		for name, d := range io {
			dev := [2]string{"device", name}
			add("node_disk_read_bytes_total", float64(d.ReadBytes), dev)
			add("node_disk_written_bytes_total", float64(d.WriteBytes), dev)
			add("node_disk_reads_completed_total", float64(d.ReadCount), dev)
			add("node_disk_writes_completed_total", float64(d.WriteCount), dev)
			add("node_disk_io_time_seconds_total", float64(d.IoTime)/1000, dev)
		}
	}
	if parts, err := disk.Partitions(false); err == nil {
		for _, p := range parts {
			u, err := disk.Usage(p.Mountpoint)
			if err != nil || u.Total == 0 {
				continue
			}
			mp := [2]string{"mountpoint", strings.TrimRight(p.Mountpoint, `\`)}
			fs := [2]string{"fstype", p.Fstype}
			add("node_filesystem_size_bytes", float64(u.Total), mp, fs)
			add("node_filesystem_avail_bytes", float64(u.Free), mp, fs)
		}
	}
	if nics, err := net.IOCounters(true); err == nil {
		for _, n := range nics {
			if n.BytesRecv == 0 && n.BytesSent == 0 {
				continue
			}
			dev := [2]string{"device", n.Name}
			add("node_network_receive_bytes_total", float64(n.BytesRecv), dev)
			add("node_network_transmit_bytes_total", float64(n.BytesSent), dev)
			add("node_network_receive_packets_total", float64(n.PacketsRecv), dev)
			add("node_network_transmit_packets_total", float64(n.PacketsSent), dev)
		}
	}
	if up, err := host.Uptime(); err == nil {
		add("node_uptime_seconds", float64(up))
	}
	if ps, err := process.Pids(); err == nil {
		add("node_processes", float64(len(ps)))
	}
	return out
}
