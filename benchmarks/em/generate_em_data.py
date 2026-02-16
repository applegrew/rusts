#!/usr/bin/env python3
"""
EM Benchmark Data Generator for RusTs

Generates InfluxDB line-protocol data that matches the EM (Endpoint
Management) schema used by DB C / DB R / MetricBase.  The output can be
piped straight into the RusTs /write endpoint.

Measurements generated
──────────────────────
  mb_em_device_metrics            – 38+ device-level fields
  mb_em_installed_app_metrics     – per-app metrics (CPU, mem, IO, crashes …)
  mb_em_web_app_metrics           – web-app experience metrics
  mb_em_network_monitoring_metrics – packet loss, latency, jitter
  mb_em_device_metrics_battery    – battery-specific metrics

Usage
─────
  python3 generate_em_data.py [OPTIONS]

  --devices N          Number of devices (default 100)
  --apps-per-device N  Installed apps per device (default 5)
  --web-apps N         Number of web apps per device (default 2)
  --hours H            Hours of data to generate (default 2)
  --interval S         Seconds between data points (default 300 = 5 min)
  --seed S             Random seed (default 42)
  --output FILE        Output file (default: stdout)
"""

import argparse
import hashlib
import math
import random
import sys
import time as _time

# ── Well-known IDs from the EM benchmark queries ──────────────────────────
# These MUST appear in the generated data so that the benchmark queries match.
KNOWN_DEVICE_ID = "0cddbe08f4f53a1047c02d117116e609"
KNOWN_APP_SYS_ID = "a61923e13347ea944feac6273e5c7bb8"
KNOWN_WEB_APP_SYS_ID = "29092fa13347ea944feac6273e5c7bc7"

# Extra app IDs referenced in Part A "device -> applications" queries
KNOWN_EXTRA_APP_IDS = [
    "0185948833fab2108124c6273e5c7ba5",
    "9985d48833fab2108124c6273e5c7bb9",
    "9585d48833fab2108124c6273e5c7b42",
    "9185d48833fab2108124c6273e5c7b9f",
    "8d85948833fab2108124c6273e5c7beb",
    "f48518cc33f6b2108124c6273e5c7be2",
    "1985d48833fab2108124c6273e5c7b6a",
    "8985d48833fab2108124c6273e5c7b08",
    "0985d48833fab2108124c6273e5c7b25",
    "1d85d48833fab2108124c6273e5c7b84",
]

# ── Time anchors (matching the benchmark queries) ──────────────────────────
# Queries use:  2025-12-24 06:13:48  to  2025-12-24 08:13:48  (2-hour window)
#               2025-12-23 08:13:48  to  2025-12-24 08:13:48  (24-hour window)
#               2025-11-24 08:13:48  to  2025-12-24 08:13:48  (30-day window)
# We generate data ending at 2025-12-24 08:13:48 UTC and going back --hours.
ANCHOR_END_UTC = "2025-12-24T08:13:48Z"


def _parse_anchor(s: str) -> int:
    """Parse ISO-ish timestamp to nanoseconds since epoch."""
    import calendar, datetime
    dt = datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
    return int(calendar.timegm(dt.timetuple())) * 1_000_000_000


def _make_id(prefix: str, index: int) -> str:
    """Deterministic 32-char hex ID."""
    return hashlib.md5(f"{prefix}-{index}".encode()).hexdigest()


def _escape_tag_value(v: str) -> str:
    """Escape special chars in line-protocol tag values."""
    return v.replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")


# ── Metric value helpers ───────────────────────────────────────────────────

def _diurnal(ts_ns: int) -> float:
    """Return a 0.3–1.3 multiplier based on simulated hour-of-day."""
    hour = ((ts_ns // 1_000_000_000) // 3600) % 24
    base = 0.5 + 0.5 * abs(math.cos((hour - 14) * math.pi / 12))
    return base * (1.3 if 9 <= hour < 18 else 0.7)


def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


# ── Generators per measurement ─────────────────────────────────────────────

def gen_device_metrics(rng: random.Random, ts_ns: int, device_id: str,
                       type_k: str, base_cpu: float, base_mem: float) -> str:
    """mb_em_device_metrics — 38 fields."""
    d = _diurnal(ts_ns)
    noise = lambda scale=5.0: rng.uniform(-scale, scale)

    cpu = _clamp(base_cpu * d + noise())
    mem = _clamp(base_mem * d + noise(3))
    disk_usage = _clamp(rng.uniform(30, 80) + noise(2))
    disk_time = _clamp(rng.uniform(5, 60) + noise(5))
    vpn = rng.choice([0, 1])
    uptime = rng.randint(3600, 864000)
    cpu_user = _clamp(cpu * rng.uniform(0.4, 0.7))
    virt_mem = _clamp(mem * rng.uniform(0.8, 1.2))
    page_file = _clamp(rng.uniform(10, 50) + noise(2))
    page_file_size = rng.randint(4096, 16384)
    mem_pages = max(0, rng.uniform(50, 500) + noise(20))
    avg_disk_read = max(0.0001, rng.uniform(0.001, 0.05) + noise(0.005))
    avg_disk_write = max(0.0001, rng.uniform(0.001, 0.05) + noise(0.005))
    avg_disk_transfer = (avg_disk_read + avg_disk_write) / 2
    disk_reads_s = max(0, rng.uniform(10, 200) + noise(20))
    disk_writes_s = max(0, rng.uniform(10, 200) + noise(20))
    avg_disk_q = max(0, rng.uniform(0, 5) + noise(0.5))
    io_read = max(0, rng.uniform(1000, 50000) + noise(5000))
    io_write = max(0, rng.uniform(1000, 50000) + noise(5000))
    wifi_sig = _clamp(rng.uniform(40, 100) + noise(5))
    wifi_rssi = rng.uniform(-80, -30) + noise(3)
    wifi_rx = max(0, rng.uniform(10, 300) + noise(20))
    wifi_tx = max(0, rng.uniform(10, 300) + noise(20))
    power = max(0, rng.uniform(5, 50) + noise(3))
    compliance = _clamp(rng.uniform(70, 100) + noise(2))
    e_total = max(0, rng.uniform(10, 80) + noise(5))
    e_cpu = e_total * rng.uniform(0.2, 0.4)
    e_gpu = e_total * rng.uniform(0.05, 0.2)
    e_disk = e_total * rng.uniform(0.05, 0.15)
    e_net = e_total * rng.uniform(0.02, 0.1)
    e_display = e_total * rng.uniform(0.1, 0.25)
    e_loss = e_total * rng.uniform(0.01, 0.05)
    e_soc = e_total * rng.uniform(0.02, 0.08)
    e_mbb = e_total * rng.uniform(0.0, 0.03)
    e_other = e_total * rng.uniform(0.01, 0.05)
    e_emi = e_total * rng.uniform(0.0, 0.02)
    e_ane = e_total * rng.uniform(0.0, 0.02)
    crashes = rng.choices([0, 0, 0, 0, 1], k=1)[0]

    tag_set = (
        f"devicesysid_k={_escape_tag_value(device_id)},"
        f"type_k={_escape_tag_value(type_k)}"
    )

    fields = (
        f"em_device_cpu_usage={cpu:.4f},"
        f"em_device_memory_usage={mem:.4f},"
        f"em_disk_usage={disk_usage:.4f},"
        f"em_disk_time={disk_time:.4f},"
        f"em_vpn_status={vpn}i,"
        f"em_device_uptime={uptime}i,"
        f"em_cpu_user_time={cpu_user:.4f},"
        f"em_virtual_memory_usage={virt_mem:.4f},"
        f"em_page_file_usage={page_file:.4f},"
        f"em_total_page_file_size={page_file_size}i,"
        f"em_memory_pages_per_sec={mem_pages:.2f},"
        f"em_avg_disk_sec_per_read={avg_disk_read:.6f},"
        f"em_avg_disk_sec_per_write={avg_disk_write:.6f},"
        f"em_avg_disk_sec_per_transfer={avg_disk_transfer:.6f},"
        f"em_disk_reads_per_sec={disk_reads_s:.2f},"
        f"em_disk_writes_per_sec={disk_writes_s:.2f},"
        f"em_avg_disk_queue_length={avg_disk_q:.4f},"
        f"em_device_io_usage_read={io_read:.2f},"
        f"em_device_io_usage_write={io_write:.2f},"
        f"em_wifi_signal_strength={wifi_sig:.2f},"
        f"em_wifi_rssi={wifi_rssi:.2f},"
        f"em_wifi_receive_rate={wifi_rx:.2f},"
        f"em_wifi_transmit_rate={wifi_tx:.2f},"
        f"em_power_consumption={power:.4f},"
        f"em_system_compliance_rating={compliance:.2f},"
        f"em_energy_consumption_total={e_total:.4f},"
        f"em_energy_consumption_cpu={e_cpu:.4f},"
        f"em_energy_consumption_gpu={e_gpu:.4f},"
        f"em_energy_consumption_disk={e_disk:.4f},"
        f"em_energy_consumption_network={e_net:.4f},"
        f"em_energy_consumption_display={e_display:.4f},"
        f"em_energy_consumption_loss={e_loss:.4f},"
        f"em_energy_consumption_soc={e_soc:.4f},"
        f"em_energy_consumption_mbb={e_mbb:.4f},"
        f"em_energy_consumption_other={e_other:.4f},"
        f"em_energy_consumption_emi={e_emi:.4f},"
        f"em_energy_consumption_ane={e_ane:.4f},"
        f"em_device_crashes={crashes}i"
    )

    return f"mb_em_device_metrics,{tag_set} {fields} {ts_ns}"


def gen_installed_app_metrics(rng: random.Random, ts_ns: int, device_id: str,
                              app_id: str, app_name: str, app_version: str,
                              type_k: str) -> str:
    """mb_em_installed_app_metrics — 10 fields."""
    d = _diurnal(ts_ns)
    cpu = _clamp(rng.uniform(1, 30) * d + rng.uniform(-2, 2))
    mem = _clamp(rng.uniform(5, 40) * d + rng.uniform(-3, 3))
    io_r = max(0, rng.uniform(100, 5000) + rng.uniform(-500, 500))
    io_w = max(0, rng.uniform(100, 5000) + rng.uniform(-500, 500))
    usage = max(0, rng.uniform(0, 1000) + rng.uniform(-50, 50))
    uptime_val = rng.randint(0, 86400)
    crashes = rng.choices([0, 0, 0, 0, 0, 1], k=1)[0]
    freezes = rng.choices([0, 0, 0, 0, 0, 1], k=1)[0]
    is_running = rng.choice([0, 1])
    last_access = ts_ns - rng.randint(0, 3600) * 1_000_000_000

    tag_set = (
        f"devicesysid_k={_escape_tag_value(device_id)},"
        f"appsysid_k={_escape_tag_value(app_id)},"
        f"appname_t={_escape_tag_value(app_name)},"
        f"appversion_t={_escape_tag_value(app_version)},"
        f"type_k={_escape_tag_value(type_k)}"
    )

    fields = (
        f"em_installed_app_cpu_usage={cpu:.4f},"
        f"em_installed_app_memory_usage={mem:.4f},"
        f"em_installed_app_io_usage_read={io_r:.2f},"
        f"em_installed_app_io_usage_write={io_w:.2f},"
        f"em_installed_app_usage={usage:.2f},"
        f"em_installed_app_uptime={uptime_val}i,"
        f"em_installed_app_crashes={crashes}i,"
        f"em_freezes={freezes}i,"
        f"em_is_running={is_running}i,"
        f"em_installed_app_last_access_time={last_access}i"
    )

    return f"mb_em_installed_app_metrics,{tag_set} {fields} {ts_ns}"


def gen_web_app_metrics(rng: random.Random, ts_ns: int, device_id: str,
                        app_id: str, type_k: str) -> str:
    """mb_em_web_app_metrics — 12 fields."""
    availability = _clamp(rng.uniform(90, 100) + rng.uniform(-2, 2))
    resp_time = max(0, rng.uniform(50, 2000) + rng.uniform(-100, 100))
    pageload = max(0, rng.uniform(100, 5000) + rng.uniform(-200, 200))
    dns = max(0, rng.uniform(5, 200) + rng.uniform(-10, 10))
    session_len = max(0, rng.uniform(60, 3600) + rng.uniform(-30, 30))
    session_cnt = rng.randint(0, 20)
    page_view = rng.randint(0, 50)
    usage = max(0, rng.uniform(0, 500) + rng.uniform(-20, 20))
    success_req = rng.randint(0, 200)
    failed_req = rng.randint(0, 10)
    last_access = ts_ns - rng.randint(0, 3600) * 1_000_000_000

    tag_set = (
        f"devicesysid_k={_escape_tag_value(device_id)},"
        f"appsysid_k={_escape_tag_value(app_id)},"
        f"type_k={_escape_tag_value(type_k)}"
    )

    fields = (
        f"em_availability={availability:.4f},"
        f"em_response_time={resp_time:.2f},"
        f"em_pageload_time={pageload:.2f},"
        f"em_dns_lookup_time={dns:.4f},"
        f"em_session_length={session_len:.2f},"
        f"em_session_count={session_cnt}i,"
        f"em_page_view={page_view}i,"
        f"em_web_app_usage={usage:.2f},"
        f"em_successful_web_requests={success_req}i,"
        f"em_failed_web_requests={failed_req}i,"
        f"em_web_app_last_access_time={last_access}i"
    )

    return f"mb_em_web_app_metrics,{tag_set} {fields} {ts_ns}"


def gen_network_metrics(rng: random.Random, ts_ns: int, device_id: str,
                        app_id: str, type_k: str) -> str:
    """mb_em_network_monitoring_metrics — 3 fields."""
    pkt_loss = _clamp(rng.uniform(0, 5) + rng.uniform(-0.5, 0.5), 0, 100)
    latency = max(0, rng.uniform(10, 200) + rng.uniform(-10, 10))
    jitter = max(0, rng.uniform(1, 30) + rng.uniform(-2, 2))

    tag_set = (
        f"devicesysid_k={_escape_tag_value(device_id)},"
        f"appsysid_k={_escape_tag_value(app_id)},"
        f"type_k={_escape_tag_value(type_k)}"
    )

    fields = (
        f"em_packet_loss={pkt_loss:.4f},"
        f"em_network_latency={latency:.4f},"
        f"em_jitter={jitter:.4f}"
    )

    return f"mb_em_network_monitoring_metrics,{tag_set} {fields} {ts_ns}"


def gen_battery_metrics(rng: random.Random, ts_ns: int, device_id: str,
                        battery_id: str, type_k: str) -> str:
    """mb_em_device_metrics_battery — battery fields."""
    charge = _clamp(rng.uniform(10, 100) + rng.uniform(-5, 5))
    health = _clamp(rng.uniform(70, 100) + rng.uniform(-2, 2))
    cycle = rng.randint(50, 1000)
    temp = rng.uniform(25, 45) + rng.uniform(-3, 3)
    voltage = rng.uniform(3.5, 4.2) + rng.uniform(-0.1, 0.1)
    status = rng.choice([0, 1, 2])  # 0=discharging, 1=charging, 2=full

    tag_set = (
        f"devicesysid_k={_escape_tag_value(device_id)},"
        f"batteryid_k={_escape_tag_value(battery_id)},"
        f"type_k={_escape_tag_value(type_k)}"
    )

    fields = (
        f"em_battery_charge_level={charge:.2f},"
        f"em_battery_health={health:.2f},"
        f"em_battery_cycle_count={cycle}i,"
        f"em_battery_temperature={temp:.2f},"
        f"em_battery_voltage={voltage:.4f},"
        f"em_battery_status={status}i"
    )

    return f"mb_em_device_metrics_battery,{tag_set} {fields} {ts_ns}"


# ── Fleet generation ───────────────────────────────────────────────────────

APP_NAMES = [
    "Microsoft Teams", "Google Chrome", "Slack", "Zoom", "Microsoft Outlook",
    "Visual Studio Code", "Adobe Acrobat", "Salesforce", "SAP GUI",
    "ServiceNow Agent", "Citrix Workspace", "OneDrive", "Dropbox",
    "Microsoft Word", "Microsoft Excel",
]

APP_VERSIONS = ["1.0.0", "1.1.0", "2.0.0", "2.1.3", "3.0.1", "3.2.0"]


def build_fleet(rng: random.Random, num_devices: int, apps_per_device: int,
                web_apps_per_device: int):
    """Return a list of device dicts with their associated apps."""
    devices = []

    for i in range(num_devices):
        # First device is always the well-known device
        if i == 0:
            did = KNOWN_DEVICE_ID
        else:
            did = _make_id("device", i)

        base_cpu = rng.uniform(15, 70)
        base_mem = rng.uniform(30, 80)

        # Installed apps
        installed_apps = []
        for j in range(apps_per_device):
            if i == 0 and j == 0:
                aid = KNOWN_APP_SYS_ID
            elif j < len(KNOWN_EXTRA_APP_IDS) and i < 3:
                aid = KNOWN_EXTRA_APP_IDS[j]
            else:
                aid = _make_id(f"app-{i}", j)
            app_name = APP_NAMES[j % len(APP_NAMES)]
            app_ver = APP_VERSIONS[j % len(APP_VERSIONS)]
            installed_apps.append({
                "id": aid, "name": app_name, "version": app_ver,
            })

        # Web apps
        web_apps = []
        for j in range(web_apps_per_device):
            if i == 0 and j == 0:
                wid = KNOWN_WEB_APP_SYS_ID
            else:
                wid = _make_id(f"webapp-{i}", j)
            web_apps.append({"id": wid})

        battery_id = _make_id(f"battery-{i}", 0)

        devices.append({
            "id": did,
            "base_cpu": base_cpu,
            "base_mem": base_mem,
            "installed_apps": installed_apps,
            "web_apps": web_apps,
            "battery_id": battery_id,
        })

    return devices


# ── Main generation loop ───────────────────────────────────────────────────

def generate(args):
    rng = random.Random(args.seed)
    anchor_end_ns = _parse_anchor(ANCHOR_END_UTC)
    start_ns = anchor_end_ns - args.hours * 3600 * 1_000_000_000
    interval_ns = args.interval * 1_000_000_000

    devices = build_fleet(rng, args.devices, args.apps_per_device,
                          args.web_apps)

    out = open(args.output, "w") if args.output else sys.stdout
    total_points = 0
    t0 = _time.monotonic()

    try:
        ts = start_ns
        while ts <= anchor_end_ns:
            for dev in devices:
                did = dev["id"]

                # 1) Device metrics
                line = gen_device_metrics(rng, ts, did, "device",
                                          dev["base_cpu"], dev["base_mem"])
                out.write(line)
                out.write("\n")
                total_points += 1

                # 2) Installed app metrics
                for app in dev["installed_apps"]:
                    line = gen_installed_app_metrics(
                        rng, ts, did, app["id"], app["name"],
                        app["version"], "installed_app")
                    out.write(line)
                    out.write("\n")
                    total_points += 1

                # 3) Web app metrics + network monitoring
                for wapp in dev["web_apps"]:
                    line = gen_web_app_metrics(rng, ts, did, wapp["id"],
                                              "web_app")
                    out.write(line)
                    out.write("\n")
                    total_points += 1

                    line = gen_network_metrics(rng, ts, did, wapp["id"],
                                              "web_app")
                    out.write(line)
                    out.write("\n")
                    total_points += 1

                # 4) Battery metrics
                line = gen_battery_metrics(rng, ts, did, dev["battery_id"],
                                          "device")
                out.write(line)
                out.write("\n")
                total_points += 1

            ts += interval_ns

        elapsed = _time.monotonic() - t0
        num_timestamps = int((anchor_end_ns - start_ns) / interval_ns) + 1
        print(f"# Generated {total_points:,} points "
              f"({args.devices} devices × {num_timestamps} timestamps) "
              f"in {elapsed:.1f}s",
              file=sys.stderr)

    finally:
        if args.output:
            out.close()


def main():
    parser = argparse.ArgumentParser(
        description="Generate EM benchmark data for RusTs")
    parser.add_argument("--devices", type=int, default=100,
                        help="Number of devices (default: 100)")
    parser.add_argument("--apps-per-device", type=int, default=5,
                        help="Installed apps per device (default: 5)")
    parser.add_argument("--web-apps", type=int, default=2,
                        help="Web apps per device (default: 2)")
    parser.add_argument("--hours", type=int, default=2,
                        help="Hours of data to generate (default: 2)")
    parser.add_argument("--interval", type=int, default=300,
                        help="Seconds between data points (default: 300)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed (default: 42)")
    parser.add_argument("--output", type=str, default=None,
                        help="Output file (default: stdout)")
    args = parser.parse_args()
    generate(args)


if __name__ == "__main__":
    main()
