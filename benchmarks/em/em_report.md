# EM Benchmark Report — RusTs

- **Iterations per query**: 5
- **Total queries**: 70
- **P95 < 2s (pass)**: 70
- **P95 >= 2s (fail)**: 0
- **Queries with errors**: 0
- **Geometric mean speedup vs DB R**: 5826.1x
- **Geometric mean speedup vs DB C**: 6092.4x

## Category Summary

| Category | Queries | P95<2s | P95>=2s | Errors |
|----------|--------:|-------:|--------:|-------:|
| A | 14 | 14 | 0 | 0 |
| B | 40 | 40 | 0 | 0 |
| C | 16 | 16 | 0 | 0 |

## Detailed Results

| ID | Name | Cat | RusTs P50 | RusTs P95 | RusTs P99 | DB R ms | DB C ms | vs DB R | vs DB C | Pass |
|----|------|-----|----------:|----------:|----------:|----------:|----------:|----------:|----------:|:----:|
| QA01 | DeviceMet - Page file usage | A | 1 | 1 | 1 | 8776 | — | 14874.6x | — | ✅ |
| QA02 | DeviceMet - Avg Disk Sec Per Write | A | 1 | 1 | 1 | 7960 | — | 12060.6x | — | ✅ |
| QA03 | DeviceMet - Memory pages per sec | A | 1 | 1 | 1 | 6922 | — | 11347.5x | — | ✅ |
| QA04 | InstalledAppl - Memory usage (raw) | A | 1 | 1 | 1 | 6572 | — | 9127.8x | — | ✅ |
| QA05 | InstalledAppl - I/O write (raw) | A | 1 | 1 | 1 | 6422 | — | 11892.6x | — | ✅ |
| QA06 | DeviceMet - Page file size | A | 1 | 1 | 1 | 5938 | — | 8862.7x | — | ✅ |
| QA07 | DeviceMet - Top 10 Memory usage | A | 1 | 1 | 1 | 4227 | — | 5636.0x | — | ✅ |
| QA08 | Dashboard - Active users (50k) | A | 4 | 4 | 4 | 3118 | — | 822.7x | — | ✅ |
| QA09 | DeviceMet - Battery percentage | A | 1 | 1 | 1 | 2594 | — | 4632.1x | — | ✅ |
| QA10 | DeviceMet - performance (Top 10 CPU by a | A | 1 | 1 | 1 | 2622 | — | 4855.6x | — | ✅ |
| QA11 | active devices | A | 4 | 4 | 4 | 1574 | — | 426.6x | — | ✅ |
| QA12 | device -> applications (web app usage) | A | 1 | 1 | 1 | 1665 | — | 2413.0x | — | ✅ |
| QA13 | device -> applications (installed app us | A | 1 | 1 | 1 | 2072 | — | 3453.3x | — | ✅ |
| QA14 | DeviceMet - Applications (MAX last acces | A | 1 | 1 | 1 | 42188 | — | 71505.1x | — | ✅ |
| QB01 | DeviceMet - Web App Packet Loss | B | 1 | 1 | 1 | — | 7427 | — | 14013.2x | ✅ |
| QB02 | DeviceMet - Top 10 Memory usage | B | 1 | 1 | 1 | — | 6666 | — | 12120.0x | ✅ |
| QB03 | DeviceMet - Top 10 CPU usage | B | 1 | 1 | 1 | — | 6569 | — | 11943.6x | ✅ |
| QB04 | DeviceMet - Web App Network Latency | B | 1 | 1 | 1 | — | 5509 | — | 10016.4x | ✅ |
| QB05 | DeviceMet - Disk energy consumption | B | 0 | 1 | 1 | — | 5127 | — | 10254.0x | ✅ |
| QB06 | DeviceMet - I/O read | B | 0 | 1 | 1 | — | 4801 | — | 10437.0x | ✅ |
| QB07 | DeviceMet - WiFi RSSI | B | 0 | 0 | 0 | — | 4631 | — | 11026.2x | ✅ |
| QB08 | DeviceMet - ANE energy consumption | B | 1 | 1 | 1 | — | 4299 | — | 7412.1x | ✅ |
| QB09 | DeviceMet - CPU usage | B | 0 | 0 | 0 | — | 4228 | — | 9395.6x | ✅ |
| QB10 | DeviceMet - User time percentage | B | 0 | 0 | 0 | — | 4220 | — | 9590.9x | ✅ |
| QB11 | DeviceMet - Memory pages per sec | B | 1 | 1 | 1 | — | 4216 | — | 8266.7x | ✅ |
| QB12 | DeviceMet - SOC energy consumption | B | 0 | 0 | 0 | — | 4129 | — | 9175.6x | ✅ |
| QB13 | DeviceMet - Page file usage | B | 0 | 0 | 0 | — | 4099 | — | 9315.9x | ✅ |
| QB14 | DeviceMet - Disk Read per sec | B | 1 | 1 | 1 | — | 4070 | — | 7267.9x | ✅ |
| QB15 | DeviceMet - EMI energy consumption | B | 0 | 0 | 0 | — | 3467 | — | 7537.0x | ✅ |
| QB16 | DeviceMet - Virtual memory | B | 0 | 0 | 0 | — | 3452 | — | 8419.5x | ✅ |
| QB17 | DeviceMet - Energy loss | B | 1 | 1 | 1 | — | 3259 | — | 6035.2x | ✅ |
| QB18 | DeviceMet - Avg Disk Sec Per Write | B | 0 | 0 | 0 | — | 3112 | — | 6915.6x | ✅ |
| QB19 | DeviceMet - WiFi receive rate | B | 0 | 0 | 0 | — | 2975 | — | 7083.3x | ✅ |
| QB20 | DeviceMet - Disk Write per sec | B | 0 | 1 | 1 | — | 2913 | — | 6774.4x | ✅ |
| QB21 | DeviceMet - Display energy consumption | B | 0 | 0 | 0 | — | 2827 | — | 6425.0x | ✅ |
| QB22 | DeviceMet - I/O write | B | 0 | 0 | 0 | — | 2785 | — | 6329.5x | ✅ |
| QB23 | DeviceMet - Disk usage | B | 1 | 1 | 1 | — | 2680 | — | 5153.8x | ✅ |
| QB24 | DeviceMet - Avg. disk queue length | B | 0 | 0 | 0 | — | 2659 | — | 5780.4x | ✅ |
| QB25 | DeviceMet - Disk time % | B | 0 | 0 | 0 | — | 2647 | — | 6155.8x | ✅ |
| QB26 | DeviceMet - WiFi transmit rate | B | 1 | 1 | 1 | — | 2621 | — | 4765.5x | ✅ |
| QB27 | Dashboard - Active devices (24h) | B | 4 | 4 | 4 | — | 2555 | — | 703.9x | ✅ |
| QB28 | DeviceMet - Network energy consumption | B | 0 | 1 | 1 | — | 2470 | — | 5145.8x | ✅ |
| QB29 | DeviceMet - CPU energy consumption | B | 0 | 1 | 1 | — | 2239 | — | 5088.6x | ✅ |
| QB30 | Dashboard - Active users (24h) | B | 4 | 4 | 4 | — | 2161 | — | 579.4x | ✅ |
| QB31 | DeviceMet - Avg Disk Sec Per Read | B | 0 | 1 | 1 | — | 1934 | — | 4297.8x | ✅ |
| QB32 | DeviceMet - Total energy consumption | B | 0 | 1 | 1 | — | 1817 | — | 3950.0x | ✅ |
| QB33 | DeviceMet - Uptime | B | 0 | 1 | 1 | — | 1816 | — | 3632.0x | ✅ |
| QB34 | DeviceMet - Page file size | B | 0 | 1 | 1 | — | 1760 | — | 3826.1x | ✅ |
| QB35 | DeviceMet - Other energy consumption | B | 0 | 0 | 0 | — | 1760 | — | 4093.0x | ✅ |
| QB36 | DeviceMet - WiFi signal strength | B | 0 | 1 | 1 | — | 1738 | — | 4138.1x | ✅ |
| QB37 | DeviceMet - Avg Disk Sec Per Transfer | B | 0 | 0 | 0 | — | 1706 | — | 3877.3x | ✅ |
| QB38 | DeviceMet - GPU energy consumption | B | 0 | 0 | 0 | — | 1700 | — | 3863.6x | ✅ |
| QB39 | DeviceMet - Memory usage | B | 1 | 1 | 1 | — | 1668 | — | 3088.9x | ✅ |
| QB40 | DeviceMet - MBB energy consumption | B | 0 | 0 | 0 | — | 1635 | — | 3478.7x | ✅ |
| QC01 | RAW - Web App Metrics (2h) | C | 1 | 1 | 1 | — | 5922 | — | 10767.3x | ✅ |
| QC02 | RAW - Installed App Metrics (2h) | C | 1 | 1 | 1 | — | 5771 | — | 10124.6x | ✅ |
| QC03 | RAW - Network Monitoring Metrics (2h) | C | 0 | 0 | 0 | — | 5251 | — | 11415.2x | ✅ |
| QC04 | RAW - Device Disk Metrics (2h) | C | 1 | 1 | 1 | — | 4345 | — | 7364.4x | ✅ |
| QC05 | RAW - Device Metrics 38 fields (2h) | C | 1 | 1 | 1 | — | 4075 | — | 4969.5x | ✅ |
| QC06 | RAW - Device Energy Metrics (2h) | C | 1 | 1 | 1 | — | 3843 | — | 6513.6x | ✅ |
| QC07 | RAW - Device CPU and Memory (2h) | C | 1 | 1 | 1 | — | 2327 | — | 4475.0x | ✅ |
| QC08 | RAW - Device WiFi Metrics (2h) | C | 1 | 1 | 1 | — | 2190 | — | 3650.0x | ✅ |
| QC09 | HOURLY - Installed App Metrics (24h) | C | 0 | 1 | 1 | — | 5819 | — | 14192.7x | ✅ |
| QC10 | ALL_AGENTS_HOURLY - Installed App Metric | C | 0 | 0 | 0 | — | 5585 | — | 13622.0x | ✅ |
| QC11 | DAILY - Installed App Metrics (30d) | C | 1 | 1 | 1 | — | 5762 | — | 11298.0x | ✅ |
| QC12 | ALL_AGENTS_DAILY - Installed App Metrics | C | 0 | 0 | 0 | — | 3156 | — | 7697.6x | ✅ |
| QC13 | HOURLY - Device Metrics 38 fields (24h) | C | 0 | 0 | 0 | — | 3647 | — | 8895.1x | ✅ |
| QC14 | DAILY - Web App Metrics (30d) | C | 0 | 0 | 0 | — | 1671 | — | 4177.5x | ✅ |
| QC15 | DAILY - Network Monitoring Metrics (30d) | C | 0 | 0 | 0 | — | 2995 | — | 8557.1x | ✅ |
| QC16 | DAILY - Device Metrics 38 fields (30d) | C | 0 | 0 | 0 | — | 564 | — | 1342.9x | ✅ |

