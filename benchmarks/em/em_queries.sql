-- ============================================================================
-- EM Benchmark Queries for RusTs
-- ============================================================================
-- Adapted from EM slow-query investigation (Parts A, B, C).
-- Each query is annotated with its source, category, and baseline timings.
--
-- Categories:
--   A = Part A (perf37, 50k load, DB R timings)
--   B = Part B (perf36, DB C single-metric timings)
--   C = Part C (perf36, DB C multi-metric timings)
--
-- Annotation format:
--   -- ID | Name | Category | DB R_ms | DB C_ms
--
-- Note: DB C uses per-metric measurements (e.g. em_packet_loss) while
-- RusTs stores everything in em_device_metrics with field names.
-- Part B queries are translated to use the RusTs schema accordingly.
-- ROLLUP queries (CREATE TEMP TABLE / DATE_BIN / subqueries) are excluded
-- as RusTs does not support those constructs.
-- ============================================================================

-- ============================================================================
-- CATEGORY A: Part A — perf37 single-metric (DB R slow queries @ 50k)
-- ============================================================================

-- QA01 | DeviceMet - Page file usage | A | 8776 | 0
SELECT time, em_page_file_usage FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_page_file_usage >= 0 AND type_k = 'device' ORDER BY time DESC

-- QA02 | DeviceMet - Avg Disk Sec Per Write | A | 7960 | 0
SELECT time, em_avg_disk_sec_per_write FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_avg_disk_sec_per_write >= 0 AND type_k = 'device' ORDER BY time DESC

-- QA03 | DeviceMet - Memory pages per sec | A | 6922 | 0
SELECT time, em_memory_pages_per_sec FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_memory_pages_per_sec >= 0 AND type_k = 'device' ORDER BY time DESC

-- QA04 | InstalledAppl - Memory usage (raw) | A | 6572 | 0
SELECT time, em_installed_app_memory_usage FROM em_installed_app_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND appsysid_k = 'a61923e13347ea944feac6273e5c7bb8' AND em_installed_app_memory_usage > 0 AND type_k = 'installed_app' ORDER BY time DESC LIMIT 100

-- QA05 | InstalledAppl - I/O write (raw) | A | 6422 | 0
SELECT time, em_installed_app_io_usage_write FROM em_installed_app_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND appsysid_k = 'a61923e13347ea944feac6273e5c7bb8' AND em_installed_app_io_usage_write > 0 AND type_k = 'installed_app' ORDER BY time DESC LIMIT 100

-- QA06 | DeviceMet - Page file size | A | 5938 | 0
SELECT time, em_total_page_file_size FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_total_page_file_size >= 0 AND type_k = 'device' ORDER BY time DESC

-- QA07 | DeviceMet - Top 10 Memory usage | A | 4227 | 0
SELECT AVG(em_installed_app_memory_usage) AS maxValue, appname_t, appversion_t FROM em_installed_app_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND type_k = 'installed_app' AND em_installed_app_memory_usage >= 0 AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' GROUP BY appname_t, appversion_t ORDER BY maxValue DESC LIMIT 10

-- QA08 | Dashboard - Active users (50k) | A | 3118 | 0
SELECT time, COUNT(DISTINCT devicesysid_k) FROM em_device_metrics WHERE em_device_cpu_usage >= 0 AND time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND type_k = 'device' GROUP BY time ORDER BY time DESC

-- QA09 | DeviceMet - Battery percentage | A | 2594 | 0
SELECT time, AVG(em_battery_charge_level), batteryid_k FROM em_device_metrics_battery WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND type_k = 'device' AND em_battery_charge_level >= 0 AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' GROUP BY batteryid_k, time ORDER BY time DESC

-- QA10 | DeviceMet - performance (Top 10 CPU by app) | A | 2622 | 0
SELECT appname_t, appversion_t, AVG(em_installed_app_cpu_usage) AS AGG_VALUE FROM em_installed_app_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND type_k = 'installed_app' GROUP BY appname_t, appversion_t ORDER BY AGG_VALUE DESC LIMIT 10

-- QA11 | active devices | A | 1574 | 0
SELECT time, COUNT(DISTINCT devicesysid_k) FROM em_device_metrics WHERE em_device_cpu_usage >= 0 AND time >= '2025-12-23 08:13:48' AND time <= '2025-12-24 08:13:48' AND type_k = 'device' GROUP BY time ORDER BY time DESC

-- QA12 | device -> applications (web app usage) | A | 1665 | 0
SELECT appsysid_k, SUM(em_web_app_usage) FROM em_web_app_metrics WHERE appsysid_k IN ('0185948833fab2108124c6273e5c7ba5', '9985d48833fab2108124c6273e5c7bb9', '9585d48833fab2108124c6273e5c7b42', '9185d48833fab2108124c6273e5c7b9f', '8d85948833fab2108124c6273e5c7beb', 'f48518cc33f6b2108124c6273e5c7be2', '1985d48833fab2108124c6273e5c7b6a', '8985d48833fab2108124c6273e5c7b08', '0985d48833fab2108124c6273e5c7b25', '1d85d48833fab2108124c6273e5c7b84') AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_web_app_usage >= 0 AND time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' GROUP BY appsysid_k

-- QA13 | device -> applications (installed app usage) | A | 2072 | 0
SELECT appsysid_k, appversion_t, SUM(em_installed_app_usage) FROM em_installed_app_metrics WHERE appsysid_k IN ('0185948833fab2108124c6273e5c7ba5', '9985d48833fab2108124c6273e5c7bb9', '9585d48833fab2108124c6273e5c7b42', '9185d48833fab2108124c6273e5c7b9f', '8d85948833fab2108124c6273e5c7beb', 'f48518cc33f6b2108124c6273e5c7be2', '1985d48833fab2108124c6273e5c7b6a', '8985d48833fab2108124c6273e5c7b08', '0985d48833fab2108124c6273e5c7b25', '1d85d48833fab2108124c6273e5c7b84') AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_installed_app_usage >= 0 AND time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' GROUP BY appsysid_k, appversion_t

-- QA14 | DeviceMet - Applications (MAX last access) | A | 42188 | 0
SELECT appsysid_k, appversion_t, MAX(em_installed_app_last_access_time) FROM em_installed_app_metrics WHERE appsysid_k IN ('0185948833fab2108124c6273e5c7ba5', '9985d48833fab2108124c6273e5c7bb9', '9585d48833fab2108124c6273e5c7b42', '9185d48833fab2108124c6273e5c7b9f', '8d85948833fab2108124c6273e5c7beb', 'f48518cc33f6b2108124c6273e5c7be2', '1985d48833fab2108124c6273e5c7b6a', '8985d48833fab2108124c6273e5c7b08', '0985d48833fab2108124c6273e5c7b25', '1d85d48833fab2108124c6273e5c7b84') AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_installed_app_last_access_time > 0 AND time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' GROUP BY appsysid_k, appversion_t

-- ============================================================================
-- CATEGORY B: Part B — perf36 single-metric (DB C timings)
-- DB C uses per-metric measurements; RusTs stores these as fields within
-- em_device_metrics. Queries are translated accordingly.
-- ============================================================================

-- QB01 | DeviceMet - Web App Packet Loss | B | 0 | 7427
SELECT time, em_packet_loss FROM em_network_monitoring_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_packet_loss >= 0 AND type_k = 'web_app' AND appsysid_k = '29092fa13347ea944feac6273e5c7bc7' ORDER BY time DESC

-- QB02 | DeviceMet - Top 10 Memory usage | B | 0 | 6666
SELECT AVG(em_installed_app_memory_usage) AS maxValue, appname_t, appversion_t FROM em_installed_app_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND type_k = 'installed_app' AND em_installed_app_memory_usage >= 0 AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' GROUP BY appname_t, appversion_t ORDER BY maxValue DESC LIMIT 10

-- QB03 | DeviceMet - Top 10 CPU usage | B | 0 | 6569
SELECT AVG(em_installed_app_cpu_usage) AS maxValue, appname_t, appversion_t FROM em_installed_app_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND type_k = 'installed_app' AND em_installed_app_cpu_usage >= 0 AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' GROUP BY appname_t, appversion_t ORDER BY maxValue DESC LIMIT 10

-- QB04 | DeviceMet - Web App Network Latency | B | 0 | 5509
SELECT time, em_network_latency FROM em_network_monitoring_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_network_latency >= 0 AND type_k = 'web_app' AND appsysid_k = '29092fa13347ea944feac6273e5c7bc7' ORDER BY time DESC

-- QB05 | DeviceMet - Disk energy consumption | B | 0 | 5127
SELECT time, em_energy_consumption_disk FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_energy_consumption_disk >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB06 | DeviceMet - I/O read | B | 0 | 4801
SELECT time, em_device_io_usage_read FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_device_io_usage_read >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB07 | DeviceMet - WiFi RSSI | B | 0 | 4631
SELECT time, em_wifi_rssi FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND type_k = 'device' ORDER BY time DESC

-- QB08 | DeviceMet - ANE energy consumption | B | 0 | 4299
SELECT time, em_energy_consumption_ane FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_energy_consumption_ane >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB09 | DeviceMet - CPU usage | B | 0 | 4228
SELECT time, em_device_cpu_usage FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_device_cpu_usage >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB10 | DeviceMet - User time percentage | B | 0 | 4220
SELECT time, em_cpu_user_time FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_cpu_user_time >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB11 | DeviceMet - Memory pages per sec | B | 0 | 4216
SELECT time, em_memory_pages_per_sec FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_memory_pages_per_sec >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB12 | DeviceMet - SOC energy consumption | B | 0 | 4129
SELECT time, em_energy_consumption_soc FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_energy_consumption_soc >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB13 | DeviceMet - Page file usage | B | 0 | 4099
SELECT time, em_page_file_usage FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_page_file_usage >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB14 | DeviceMet - Disk Read per sec | B | 0 | 4070
SELECT time, em_disk_reads_per_sec FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_disk_reads_per_sec >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB15 | DeviceMet - EMI energy consumption | B | 0 | 3467
SELECT time, em_energy_consumption_emi FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_energy_consumption_emi >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB16 | DeviceMet - Virtual memory | B | 0 | 3452
SELECT time, em_virtual_memory_usage FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_virtual_memory_usage >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB17 | DeviceMet - Energy loss | B | 0 | 3259
SELECT time, em_energy_consumption_loss FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_energy_consumption_loss >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB18 | DeviceMet - Avg Disk Sec Per Write | B | 0 | 3112
SELECT time, em_avg_disk_sec_per_write FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_avg_disk_sec_per_write >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB19 | DeviceMet - WiFi receive rate | B | 0 | 2975
SELECT time, em_wifi_receive_rate FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_wifi_receive_rate >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB20 | DeviceMet - Disk Write per sec | B | 0 | 2913
SELECT time, em_disk_writes_per_sec FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_disk_writes_per_sec >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB21 | DeviceMet - Display energy consumption | B | 0 | 2827
SELECT time, em_energy_consumption_display FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_energy_consumption_display >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB22 | DeviceMet - I/O write | B | 0 | 2785
SELECT time, em_device_io_usage_write FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_device_io_usage_write >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB23 | DeviceMet - Disk usage | B | 0 | 2680
SELECT time, em_disk_usage FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_disk_usage >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB24 | DeviceMet - Avg. disk queue length | B | 0 | 2659
SELECT time, em_avg_disk_queue_length FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_avg_disk_queue_length >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB25 | DeviceMet - Disk time % | B | 0 | 2647
SELECT time, em_disk_time FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_disk_time >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB26 | DeviceMet - WiFi transmit rate | B | 0 | 2621
SELECT time, em_wifi_transmit_rate FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_wifi_transmit_rate >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB27 | Dashboard - Active devices (24h) | B | 0 | 2555
SELECT time, COUNT(DISTINCT devicesysid_k) FROM em_device_metrics WHERE em_device_cpu_usage >= 0 AND time >= '2025-12-23 08:13:48' AND time <= '2025-12-24 08:13:48' AND type_k = 'device' GROUP BY time ORDER BY time DESC

-- QB28 | DeviceMet - Network energy consumption | B | 0 | 2470
SELECT time, em_energy_consumption_network FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_energy_consumption_network >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB29 | DeviceMet - CPU energy consumption | B | 0 | 2239
SELECT time, em_energy_consumption_cpu FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_energy_consumption_cpu >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB30 | Dashboard - Active users (24h) | B | 0 | 2161
SELECT time, COUNT(DISTINCT devicesysid_k) FROM em_device_metrics WHERE em_device_cpu_usage >= 0 AND time >= '2025-12-23 08:13:48' AND time <= '2025-12-24 08:13:48' AND type_k = 'device' GROUP BY time ORDER BY time DESC

-- QB31 | DeviceMet - Avg Disk Sec Per Read | B | 0 | 1934
SELECT time, em_avg_disk_sec_per_read FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_avg_disk_sec_per_read >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB32 | DeviceMet - Total energy consumption | B | 0 | 1817
SELECT time, em_energy_consumption_total FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_energy_consumption_total >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB33 | DeviceMet - Uptime | B | 0 | 1816
SELECT time, em_device_uptime FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_device_uptime >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB34 | DeviceMet - Page file size | B | 0 | 1760
SELECT time, em_total_page_file_size FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_total_page_file_size >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB35 | DeviceMet - Other energy consumption | B | 0 | 1760
SELECT time, em_energy_consumption_other FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_energy_consumption_other >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB36 | DeviceMet - WiFi signal strength | B | 0 | 1738
SELECT time, em_wifi_signal_strength FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_wifi_signal_strength >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB37 | DeviceMet - Avg Disk Sec Per Transfer | B | 0 | 1706
SELECT time, em_avg_disk_sec_per_transfer FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_avg_disk_sec_per_transfer >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB38 | DeviceMet - GPU energy consumption | B | 0 | 1700
SELECT time, em_energy_consumption_gpu FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_energy_consumption_gpu >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB39 | DeviceMet - Memory usage | B | 0 | 1668
SELECT time, em_device_memory_usage FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_device_memory_usage >= 0 AND type_k = 'device' ORDER BY time DESC

-- QB40 | DeviceMet - MBB energy consumption | B | 0 | 1635
SELECT time, em_energy_consumption_mbb FROM em_device_metrics WHERE time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' AND devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND em_energy_consumption_mbb >= 0 AND type_k = 'device' ORDER BY time DESC

-- ============================================================================
-- CATEGORY C: Part C — perf36 multi-metric (DB C timings)
-- Multi-field SELECTs across raw / hourly / daily / all_agents tables.
-- In RusTs these map to em_device_metrics, em_installed_app_metrics,
-- em_web_app_metrics, and em_network_monitoring_metrics.
-- ============================================================================

-- QC01 | RAW - Web App Metrics (2h) | C | 0 | 5922
SELECT time, em_availability, em_response_time, em_pageload_time, em_dns_lookup_time, em_session_length, em_session_count, em_page_view, em_web_app_usage, em_successful_web_requests, em_failed_web_requests, em_web_app_last_access_time FROM em_web_app_metrics WHERE devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND appsysid_k = '29092fa13347ea944feac6273e5c7bc7' AND type_k = 'web_app' AND time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' ORDER BY time DESC LIMIT 100

-- QC02 | RAW - Installed App Metrics (2h) | C | 0 | 5771
SELECT time, em_installed_app_cpu_usage, em_installed_app_memory_usage, em_installed_app_io_usage_read, em_installed_app_io_usage_write, em_installed_app_usage, em_installed_app_uptime, em_installed_app_crashes, em_freezes, em_is_running, em_installed_app_last_access_time FROM em_installed_app_metrics WHERE devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND appsysid_k = 'a61923e13347ea944feac6273e5c7bb8' AND type_k = 'installed_app' AND time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' ORDER BY time DESC LIMIT 100

-- QC03 | RAW - Network Monitoring Metrics (2h) | C | 0 | 5251
SELECT time, em_packet_loss, em_network_latency, em_jitter FROM em_network_monitoring_metrics WHERE devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND appsysid_k = '29092fa13347ea944feac6273e5c7bc7' AND type_k = 'web_app' AND time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' ORDER BY time DESC LIMIT 100

-- QC04 | RAW - Device Disk Metrics (2h) | C | 0 | 4345
SELECT time, em_disk_usage, em_disk_time, em_avg_disk_sec_per_read, em_avg_disk_sec_per_write, em_avg_disk_sec_per_transfer, em_disk_reads_per_sec, em_disk_writes_per_sec, em_avg_disk_queue_length, em_device_io_usage_read, em_device_io_usage_write FROM em_device_metrics WHERE devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND type_k = 'device' AND time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' ORDER BY time DESC LIMIT 100

-- QC05 | RAW - Device Metrics 38 fields (2h) | C | 0 | 4075
SELECT time, em_device_cpu_usage, em_device_memory_usage, em_disk_usage, em_disk_time, em_vpn_status, em_device_uptime, em_cpu_user_time, em_virtual_memory_usage, em_page_file_usage, em_total_page_file_size, em_memory_pages_per_sec, em_avg_disk_sec_per_read, em_avg_disk_sec_per_write, em_avg_disk_sec_per_transfer, em_disk_reads_per_sec, em_disk_writes_per_sec, em_avg_disk_queue_length, em_device_io_usage_read, em_device_io_usage_write, em_wifi_signal_strength, em_wifi_rssi, em_wifi_receive_rate, em_wifi_transmit_rate, em_power_consumption, em_system_compliance_rating, em_energy_consumption_total, em_energy_consumption_cpu, em_energy_consumption_gpu, em_energy_consumption_disk, em_energy_consumption_network, em_energy_consumption_display, em_device_crashes FROM em_device_metrics WHERE devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND type_k = 'device' AND time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' ORDER BY time DESC LIMIT 100

-- QC06 | RAW - Device Energy Metrics (2h) | C | 0 | 3843
SELECT time, em_power_consumption, em_energy_consumption_total, em_energy_consumption_loss, em_energy_consumption_cpu, em_energy_consumption_gpu, em_energy_consumption_soc, em_energy_consumption_disk, em_energy_consumption_network, em_energy_consumption_display, em_energy_consumption_mbb, em_energy_consumption_other, em_energy_consumption_emi, em_energy_consumption_ane FROM em_device_metrics WHERE devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND type_k = 'device' AND time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' ORDER BY time DESC LIMIT 100

-- QC07 | RAW - Device CPU and Memory (2h) | C | 0 | 2327
SELECT time, em_device_cpu_usage, em_cpu_user_time, em_device_memory_usage, em_virtual_memory_usage, em_page_file_usage, em_memory_pages_per_sec FROM em_device_metrics WHERE devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND type_k = 'device' AND time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' ORDER BY time DESC LIMIT 100

-- QC08 | RAW - Device WiFi Metrics (2h) | C | 0 | 2190
SELECT time, em_wifi_signal_strength, em_wifi_rssi, em_wifi_receive_rate, em_wifi_transmit_rate FROM em_device_metrics WHERE devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND type_k = 'device' AND time >= '2025-12-24 06:13:48' AND time <= '2025-12-24 08:13:48' ORDER BY time DESC LIMIT 100

-- QC09 | HOURLY - Installed App Metrics (24h) | C | 0 | 5819
SELECT time, AVG(em_installed_app_cpu_usage) AS cpu_avg, AVG(em_installed_app_memory_usage) AS mem_avg, AVG(em_installed_app_io_usage_read) AS io_r_avg, AVG(em_installed_app_io_usage_write) AS io_w_avg, SUM(em_installed_app_usage) AS usage_sum, SUM(em_installed_app_crashes) AS crashes_sum, SUM(em_freezes) AS freezes_sum FROM em_installed_app_metrics WHERE devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND appsysid_k = 'a61923e13347ea944feac6273e5c7bb8' AND type_k = 'installed_app' AND time >= '2025-12-23 08:13:48' AND time <= '2025-12-24 08:13:48' GROUP BY time ORDER BY time DESC LIMIT 100

-- QC10 | ALL_AGENTS_HOURLY - Installed App Metrics (24h) | C | 0 | 5585
SELECT time, AVG(em_installed_app_cpu_usage) AS cpu_avg, AVG(em_installed_app_memory_usage) AS mem_avg, AVG(em_installed_app_io_usage_read) AS io_r_avg, AVG(em_installed_app_io_usage_write) AS io_w_avg, SUM(em_installed_app_usage) AS usage_sum, SUM(em_installed_app_crashes) AS crashes_sum, SUM(em_freezes) AS freezes_sum FROM em_installed_app_metrics WHERE appsysid_k = 'a61923e13347ea944feac6273e5c7bb8' AND type_k = 'installed_app' AND time >= '2025-12-23 08:13:48' AND time <= '2025-12-24 08:13:48' GROUP BY time ORDER BY time DESC LIMIT 100

-- QC11 | DAILY - Installed App Metrics (30d) | C | 0 | 5762
SELECT time, AVG(em_installed_app_cpu_usage) AS cpu_avg, AVG(em_installed_app_memory_usage) AS mem_avg, AVG(em_installed_app_io_usage_read) AS io_r_avg, AVG(em_installed_app_io_usage_write) AS io_w_avg, SUM(em_installed_app_usage) AS usage_sum, SUM(em_installed_app_crashes) AS crashes_sum, SUM(em_freezes) AS freezes_sum FROM em_installed_app_metrics WHERE devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND appsysid_k = 'a61923e13347ea944feac6273e5c7bb8' AND type_k = 'installed_app' AND time >= '2025-11-24 08:13:48' AND time <= '2025-12-24 08:13:48' GROUP BY time ORDER BY time DESC LIMIT 100

-- QC12 | ALL_AGENTS_DAILY - Installed App Metrics (30d) | C | 0 | 3156
SELECT time, AVG(em_installed_app_cpu_usage) AS cpu_avg, AVG(em_installed_app_memory_usage) AS mem_avg, AVG(em_installed_app_io_usage_read) AS io_r_avg, AVG(em_installed_app_io_usage_write) AS io_w_avg, SUM(em_installed_app_usage) AS usage_sum, SUM(em_installed_app_crashes) AS crashes_sum, SUM(em_freezes) AS freezes_sum FROM em_installed_app_metrics WHERE appsysid_k = 'a61923e13347ea944feac6273e5c7bb8' AND type_k = 'installed_app' AND time >= '2025-11-24 08:13:48' AND time <= '2025-12-24 08:13:48' GROUP BY time ORDER BY time DESC LIMIT 100

-- QC13 | HOURLY - Device Metrics 38 fields (24h) | C | 0 | 3647
SELECT time, AVG(em_device_cpu_usage) AS cpu_avg, AVG(em_device_memory_usage) AS mem_avg, AVG(em_disk_time) AS disk_time_avg, AVG(em_energy_consumption_total) AS energy_total_avg, AVG(em_energy_consumption_cpu) AS energy_cpu_avg, AVG(em_wifi_signal_strength) AS wifi_sig_avg, SUM(em_device_uptime) AS uptime_sum, SUM(em_device_crashes) AS crashes_sum FROM em_device_metrics WHERE devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND type_k = 'device' AND time >= '2025-12-23 08:13:48' AND time <= '2025-12-24 08:13:48' GROUP BY time ORDER BY time DESC LIMIT 100

-- QC14 | DAILY - Web App Metrics (30d) | C | 0 | 1671
SELECT time, AVG(em_availability) AS avail_avg, AVG(em_response_time) AS resp_avg, AVG(em_pageload_time) AS pageload_avg, AVG(em_dns_lookup_time) AS dns_avg, AVG(em_session_length) AS session_len_avg, SUM(em_web_app_usage) AS usage_sum, SUM(em_successful_web_requests) AS success_sum, SUM(em_failed_web_requests) AS failed_sum FROM em_web_app_metrics WHERE devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND appsysid_k = '29092fa13347ea944feac6273e5c7bc7' AND type_k = 'web_app' AND time >= '2025-11-24 08:13:48' AND time <= '2025-12-24 08:13:48' GROUP BY time ORDER BY time DESC LIMIT 100

-- QC15 | DAILY - Network Monitoring Metrics (30d) | C | 0 | 2995
SELECT time, AVG(em_packet_loss) AS pkt_loss_avg, AVG(em_network_latency) AS latency_avg, AVG(em_jitter) AS jitter_avg FROM em_network_monitoring_metrics WHERE devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND appsysid_k = '29092fa13347ea944feac6273e5c7bc7' AND type_k = 'web_app' AND time >= '2025-11-24 08:13:48' AND time <= '2025-12-24 08:13:48' GROUP BY time ORDER BY time DESC LIMIT 100

-- QC16 | DAILY - Device Metrics 38 fields (30d) | C | 0 | 564
SELECT time, AVG(em_device_cpu_usage) AS cpu_avg, AVG(em_device_memory_usage) AS mem_avg, AVG(em_disk_time) AS disk_time_avg, AVG(em_energy_consumption_total) AS energy_total_avg, AVG(em_energy_consumption_cpu) AS energy_cpu_avg, AVG(em_wifi_signal_strength) AS wifi_sig_avg, SUM(em_device_uptime) AS uptime_sum, SUM(em_device_crashes) AS crashes_sum FROM em_device_metrics WHERE devicesysid_k = '0cddbe08f4f53a1047c02d117116e609' AND type_k = 'device' AND time >= '2025-11-24 08:13:48' AND time <= '2025-12-24 08:13:48' GROUP BY time ORDER BY time DESC LIMIT 100
