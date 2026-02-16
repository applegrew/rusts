# EM Benchmark Report — RusTs

- **Iterations per query**: 5
- **Total queries**: 70
- **P95 < 2s (pass)**: 70
- **P95 >= 2s (fail)**: 0
- **Queries with errors**: 0
- **Geometric mean speedup vs DB R**: 3015.9x
- **Geometric mean speedup vs DB C**: 3998.6x

## Category Summary

| Category | Queries | P95<2s | P95>=2s | Errors |
|----------|--------:|-------:|--------:|-------:|
| A | 14 | 14 | 0 | 0 |
| B | 40 | 40 | 0 | 0 |
| C | 16 | 16 | 0 | 0 |

## Detailed Results

| ID | Name | Cat | RusTs P50 | RusTs P95 | RusTs P99 | DB R ms | DB C ms | vs DB R | vs DB C | Pass |
|----|------|-----|----------:|----------:|----------:|----------:|----------:|----------:|----------:|:----:|
| QA01 | DeviceMet - Page file usage | A | 1 | 1 | 1 | 8776 | — | 10970.0x | — | ✅ |
| QA02 | DeviceMet - Avg Disk Sec Per Write | A | 1 | 1 | 1 | 7960 | — | 11880.6x | — | ✅ |
| QA03 | DeviceMet - Memory pages per sec | A | 1 | 1 | 1 | 6922 | — | 11732.2x | — | ✅ |
| QA04 | InstalledAppl - Memory usage (raw) | A | 1 | 1 | 1 | 6572 | — | 7468.2x | — | ✅ |
| QA05 | InstalledAppl - I/O write (raw) | A | 1 | 1 | 1 | 6422 | — | 6760.0x | — | ✅ |
| QA06 | DeviceMet - Page file size | A | 1 | 1 | 1 | 5938 | — | 9734.4x | — | ✅ |
| QA07 | DeviceMet - Top 10 Memory usage | A | 1 | 1 | 1 | 4227 | — | 5953.5x | — | ✅ |
| QA08 | Dashboard - Active users (50k) | A | 36 | 38 | 39 | 3118 | — | 87.5x | — | ✅ |
| QA09 | DeviceMet - Battery percentage | A | 1 | 1 | 1 | 2594 | — | 4323.3x | — | ✅ |
| QA10 | DeviceMet - performance (Top 10 CPU by a | A | 1 | 1 | 1 | 2622 | — | 3084.7x | — | ✅ |
| QA11 | active devices | A | 553 | 563 | 565 | 1574 | — | 2.8x | — | ✅ |
| QA12 | device -> applications (web app usage) | A | 1 | 1 | 1 | 1665 | — | 2312.5x | — | ✅ |
| QA13 | device -> applications (installed app us | A | 1 | 1 | 1 | 2072 | — | 2877.8x | — | ✅ |
| QA14 | DeviceMet - Applications (MAX last acces | A | 1 | 1 | 1 | 42188 | — | 52084.0x | — | ✅ |
| QB01 | DeviceMet - Web App Packet Loss | B | 1 | 1 | 1 | — | 7427 | — | 12175.4x | ✅ |
| QB02 | DeviceMet - Top 10 Memory usage | B | 1 | 1 | 1 | — | 6666 | — | 9008.1x | ✅ |
| QB03 | DeviceMet - Top 10 CPU usage | B | 1 | 1 | 1 | — | 6569 | — | 8531.2x | ✅ |
| QB04 | DeviceMet - Web App Network Latency | B | 1 | 1 | 1 | — | 5509 | — | 7984.1x | ✅ |
| QB05 | DeviceMet - Disk energy consumption | B | 1 | 1 | 1 | — | 5127 | — | 7768.2x | ✅ |
| QB06 | DeviceMet - I/O read | B | 1 | 1 | 1 | — | 4801 | — | 8137.3x | ✅ |
| QB07 | DeviceMet - WiFi RSSI | B | 1 | 1 | 1 | — | 4631 | — | 7718.3x | ✅ |
| QB08 | DeviceMet - ANE energy consumption | B | 1 | 1 | 1 | — | 4299 | — | 7047.5x | ✅ |
| QB09 | DeviceMet - CPU usage | B | 1 | 1 | 1 | — | 4228 | — | 7829.6x | ✅ |
| QB10 | DeviceMet - User time percentage | B | 1 | 1 | 1 | — | 4220 | — | 7535.7x | ✅ |
| QB11 | DeviceMet - Memory pages per sec | B | 0 | 0 | 0 | — | 4216 | — | 9804.7x | ✅ |
| QB12 | DeviceMet - SOC energy consumption | B | 0 | 0 | 0 | — | 4129 | — | 8602.1x | ✅ |
| QB13 | DeviceMet - Page file usage | B | 0 | 0 | 0 | — | 4099 | — | 10510.3x | ✅ |
| QB14 | DeviceMet - Disk Read per sec | B | 1 | 1 | 1 | — | 4070 | — | 7400.0x | ✅ |
| QB15 | DeviceMet - EMI energy consumption | B | 0 | 1 | 1 | — | 3467 | — | 7222.9x | ✅ |
| QB16 | DeviceMet - Virtual memory | B | 0 | 0 | 0 | — | 3452 | — | 7504.3x | ✅ |
| QB17 | DeviceMet - Energy loss | B | 0 | 1 | 1 | — | 3259 | — | 6934.0x | ✅ |
| QB18 | DeviceMet - Avg Disk Sec Per Write | B | 0 | 1 | 1 | — | 3112 | — | 6483.3x | ✅ |
| QB19 | DeviceMet - WiFi receive rate | B | 0 | 1 | 1 | — | 2975 | — | 6197.9x | ✅ |
| QB20 | DeviceMet - Disk Write per sec | B | 0 | 1 | 1 | — | 2913 | — | 6473.3x | ✅ |
| QB21 | DeviceMet - Display energy consumption | B | 0 | 0 | 0 | — | 2827 | — | 6145.7x | ✅ |
| QB22 | DeviceMet - I/O write | B | 1 | 1 | 1 | — | 2785 | — | 4886.0x | ✅ |
| QB23 | DeviceMet - Disk usage | B | 0 | 1 | 1 | — | 2680 | — | 5702.1x | ✅ |
| QB24 | DeviceMet - Avg. disk queue length | B | 0 | 1 | 1 | — | 2659 | — | 5780.4x | ✅ |
| QB25 | DeviceMet - Disk time % | B | 1 | 1 | 1 | — | 2647 | — | 4643.9x | ✅ |
| QB26 | DeviceMet - WiFi transmit rate | B | 0 | 0 | 0 | — | 2621 | — | 5576.6x | ✅ |
| QB27 | Dashboard - Active devices (24h) | B | 555 | 558 | 558 | — | 2555 | — | 4.6x | ✅ |
| QB28 | DeviceMet - Network energy consumption | B | 1 | 1 | 1 | — | 2470 | — | 3528.6x | ✅ |
| QB29 | DeviceMet - CPU energy consumption | B | 1 | 1 | 1 | — | 2239 | — | 3292.6x | ✅ |
| QB30 | Dashboard - Active users (24h) | B | 558 | 560 | 561 | — | 2161 | — | 3.9x | ✅ |
| QB31 | DeviceMet - Avg Disk Sec Per Read | B | 1 | 1 | 1 | — | 1934 | — | 2330.1x | ✅ |
| QB32 | DeviceMet - Total energy consumption | B | 1 | 1 | 1 | — | 1817 | — | 2455.4x | ✅ |
| QB33 | DeviceMet - Uptime | B | 1 | 1 | 1 | — | 1816 | — | 2487.7x | ✅ |
| QB34 | DeviceMet - Page file size | B | 1 | 1 | 1 | — | 1760 | — | 2411.0x | ✅ |
| QB35 | DeviceMet - Other energy consumption | B | 1 | 1 | 1 | — | 1760 | — | 3087.7x | ✅ |
| QB36 | DeviceMet - WiFi signal strength | B | 1 | 1 | 1 | — | 1738 | — | 2633.3x | ✅ |
| QB37 | DeviceMet - Avg Disk Sec Per Transfer | B | 1 | 1 | 1 | — | 1706 | — | 2624.6x | ✅ |
| QB38 | DeviceMet - GPU energy consumption | B | 1 | 1 | 1 | — | 1700 | — | 2500.0x | ✅ |
| QB39 | DeviceMet - Memory usage | B | 1 | 1 | 1 | — | 1668 | — | 2647.6x | ✅ |
| QB40 | DeviceMet - MBB energy consumption | B | 0 | 0 | 0 | — | 1635 | — | 3406.2x | ✅ |
| QC01 | RAW - Web App Metrics (2h) | C | 1 | 1 | 1 | — | 5922 | — | 9253.1x | ✅ |
| QC02 | RAW - Installed App Metrics (2h) | C | 1 | 1 | 1 | — | 5771 | — | 7905.5x | ✅ |
| QC03 | RAW - Network Monitoring Metrics (2h) | C | 1 | 1 | 1 | — | 5251 | — | 9547.3x | ✅ |
| QC04 | RAW - Device Disk Metrics (2h) | C | 1 | 1 | 1 | — | 4345 | — | 7123.0x | ✅ |
| QC05 | RAW - Device Metrics 38 fields (2h) | C | 1 | 2 | 3 | — | 4075 | — | 3918.3x | ✅ |
| QC06 | RAW - Device Energy Metrics (2h) | C | 1 | 1 | 1 | — | 3843 | — | 5490.0x | ✅ |
| QC07 | RAW - Device CPU and Memory (2h) | C | 1 | 1 | 1 | — | 2327 | — | 3525.8x | ✅ |
| QC08 | RAW - Device WiFi Metrics (2h) | C | 1 | 1 | 1 | — | 2190 | — | 3981.8x | ✅ |
| QC09 | HOURLY - Installed App Metrics (24h) | C | 1 | 1 | 1 | — | 5819 | — | 7096.3x | ✅ |
| QC10 | ALL_AGENTS_HOURLY - Installed App Metric | C | 1 | 1 | 1 | — | 5585 | — | 7160.3x | ✅ |
| QC11 | DAILY - Installed App Metrics (30d) | C | 1 | 1 | 1 | — | 5762 | — | 7893.2x | ✅ |
| QC12 | ALL_AGENTS_DAILY - Installed App Metrics | C | 1 | 1 | 1 | — | 3156 | — | 3945.0x | ✅ |
| QC13 | HOURLY - Device Metrics 38 fields (24h) | C | 1 | 1 | 1 | — | 3647 | — | 3408.4x | ✅ |
| QC14 | DAILY - Web App Metrics (30d) | C | 1 | 1 | 1 | — | 1671 | — | 2739.3x | ✅ |
| QC15 | DAILY - Network Monitoring Metrics (30d) | C | 1 | 1 | 1 | — | 2995 | — | 5348.2x | ✅ |
| QC16 | DAILY - Device Metrics 38 fields (30d) | C | 1 | 1 | 1 | — | 564 | — | 503.6x | ✅ |

