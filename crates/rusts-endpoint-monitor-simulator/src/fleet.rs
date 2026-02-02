//! Device fleet generator for endpoint monitoring simulation.

use rand::prelude::*;
use serde::{Deserialize, Serialize};

/// Device types in the fleet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeviceType {
    Laptop,
    Desktop,
    Virtual,
}

impl DeviceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            DeviceType::Laptop => "laptop",
            DeviceType::Desktop => "desktop",
            DeviceType::Virtual => "virtual",
        }
    }

    /// Returns whether this device type has battery metrics.
    pub fn has_battery(&self) -> bool {
        matches!(self, DeviceType::Laptop)
    }
}

/// Operating system versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OsVersion {
    Windows11,
    Windows10,
    MacOS14,
    MacOS13,
    Ubuntu2204,
}

impl OsVersion {
    pub fn as_str(&self) -> &'static str {
        match self {
            OsVersion::Windows11 => "Windows 11",
            OsVersion::Windows10 => "Windows 10",
            OsVersion::MacOS14 => "macOS 14",
            OsVersion::MacOS13 => "macOS 13",
            OsVersion::Ubuntu2204 => "Ubuntu 22.04",
        }
    }
}

/// Departments in the organization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Department {
    Engineering,
    Sales,
    Marketing,
    HR,
    Finance,
    Support,
    IT,
    Legal,
    Operations,
    Executive,
}

impl Department {
    pub fn as_str(&self) -> &'static str {
        match self {
            Department::Engineering => "Engineering",
            Department::Sales => "Sales",
            Department::Marketing => "Marketing",
            Department::HR => "HR",
            Department::Finance => "Finance",
            Department::Support => "Support",
            Department::IT => "IT",
            Department::Legal => "Legal",
            Department::Operations => "Operations",
            Department::Executive => "Executive",
        }
    }

    /// Returns all departments.
    pub fn all() -> &'static [Department] {
        &[
            Department::Engineering,
            Department::Sales,
            Department::Marketing,
            Department::HR,
            Department::Finance,
            Department::Support,
            Department::IT,
            Department::Legal,
            Department::Operations,
            Department::Executive,
        ]
    }
}

/// Office locations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Location {
    NYC,
    SFO,
    LON,
    BER,
    TYO,
}

impl Location {
    pub fn as_str(&self) -> &'static str {
        match self {
            Location::NYC => "NYC",
            Location::SFO => "SFO",
            Location::LON => "LON",
            Location::BER => "BER",
            Location::TYO => "TYO",
        }
    }

    /// Returns all locations.
    pub fn all() -> &'static [Location] {
        &[
            Location::NYC,
            Location::SFO,
            Location::LON,
            Location::BER,
            Location::TYO,
        ]
    }
}

/// Applications monitored on endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Application {
    Chrome,
    Slack,
    Teams,
    Zoom,
    VSCode,
    Outlook,
    Excel,
    Word,
    Salesforce,
    Jira,
    Confluence,
    GitHub,
    Docker,
    IntelliJ,
    Figma,
}

impl Application {
    pub fn as_str(&self) -> &'static str {
        match self {
            Application::Chrome => "Chrome",
            Application::Slack => "Slack",
            Application::Teams => "Teams",
            Application::Zoom => "Zoom",
            Application::VSCode => "VSCode",
            Application::Outlook => "Outlook",
            Application::Excel => "Excel",
            Application::Word => "Word",
            Application::Salesforce => "Salesforce",
            Application::Jira => "Jira",
            Application::Confluence => "Confluence",
            Application::GitHub => "GitHub",
            Application::Docker => "Docker",
            Application::IntelliJ => "IntelliJ",
            Application::Figma => "Figma",
        }
    }

    /// Returns all applications.
    pub fn all() -> &'static [Application] {
        &[
            Application::Chrome,
            Application::Slack,
            Application::Teams,
            Application::Zoom,
            Application::VSCode,
            Application::Outlook,
            Application::Excel,
            Application::Word,
            Application::Salesforce,
            Application::Jira,
            Application::Confluence,
            Application::GitHub,
            Application::Docker,
            Application::IntelliJ,
            Application::Figma,
        ]
    }

    /// Returns applications typical for a department.
    pub fn for_department(dept: Department) -> &'static [Application] {
        match dept {
            Department::Engineering => &[
                Application::Chrome,
                Application::Slack,
                Application::VSCode,
                Application::GitHub,
                Application::Docker,
                Application::IntelliJ,
                Application::Jira,
                Application::Confluence,
            ],
            Department::Sales => &[
                Application::Chrome,
                Application::Slack,
                Application::Zoom,
                Application::Outlook,
                Application::Salesforce,
                Application::Excel,
            ],
            Department::Marketing => &[
                Application::Chrome,
                Application::Slack,
                Application::Zoom,
                Application::Figma,
                Application::Word,
                Application::Excel,
            ],
            Department::HR | Department::Legal | Department::Operations => &[
                Application::Chrome,
                Application::Slack,
                Application::Outlook,
                Application::Word,
                Application::Excel,
                Application::Teams,
            ],
            Department::Finance => &[
                Application::Chrome,
                Application::Slack,
                Application::Outlook,
                Application::Excel,
                Application::Word,
            ],
            Department::Support => &[
                Application::Chrome,
                Application::Slack,
                Application::Zoom,
                Application::Jira,
                Application::Confluence,
            ],
            Department::IT => &[
                Application::Chrome,
                Application::Slack,
                Application::VSCode,
                Application::Docker,
                Application::Jira,
                Application::Teams,
            ],
            Department::Executive => &[
                Application::Chrome,
                Application::Slack,
                Application::Zoom,
                Application::Outlook,
                Application::Excel,
                Application::Word,
            ],
        }
    }
}

/// Network connection types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionType {
    Wifi,
    Ethernet,
    Vpn,
}

impl ConnectionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ConnectionType::Wifi => "wifi",
            ConnectionType::Ethernet => "ethernet",
            ConnectionType::Vpn => "vpn",
        }
    }
}

/// A simulated device in the fleet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Device {
    /// Unique device identifier (e.g., "device-0001")
    pub id: String,

    /// Device type (laptop, desktop, virtual)
    pub device_type: DeviceType,

    /// Operating system version
    pub os_version: OsVersion,

    /// Department the device belongs to
    pub department: Department,

    /// Office location
    pub location: Location,

    /// Primary network connection type
    pub connection_type: ConnectionType,

    /// Applications installed on this device
    pub applications: Vec<Application>,

    /// Whether this device is currently in an anomalous state (5% of fleet)
    pub is_anomalous: bool,

    /// Base performance characteristics (varies per device)
    pub base_cpu: f64,
    pub base_memory: f64,
    pub base_disk: f64,
}

impl Device {
    /// Returns the device ID formatted for use in line protocol tags.
    pub fn tag_id(&self) -> &str {
        &self.id
    }
}

/// Generates a fleet of simulated devices.
pub fn generate_fleet(count: usize) -> Vec<Device> {
    let mut rng = rand::thread_rng();
    let mut devices = Vec::with_capacity(count);

    // Distribution weights
    let device_types = [
        (DeviceType::Laptop, 60),
        (DeviceType::Desktop, 30),
        (DeviceType::Virtual, 10),
    ];

    let os_versions = [
        (OsVersion::Windows11, 40),
        (OsVersion::Windows10, 25),
        (OsVersion::MacOS14, 20),
        (OsVersion::MacOS13, 10),
        (OsVersion::Ubuntu2204, 5),
    ];

    for i in 0..count {
        let device_type = weighted_choice(&mut rng, &device_types);
        let os_version = weighted_choice(&mut rng, &os_versions);
        let department = *Department::all().choose(&mut rng).unwrap();
        let location = *Location::all().choose(&mut rng).unwrap();

        // Connection type based on device type
        let connection_type = match device_type {
            DeviceType::Desktop => ConnectionType::Ethernet,
            DeviceType::Virtual => ConnectionType::Ethernet,
            DeviceType::Laptop => {
                if rng.gen_bool(0.6) {
                    ConnectionType::Wifi
                } else if rng.gen_bool(0.5) {
                    ConnectionType::Vpn
                } else {
                    ConnectionType::Ethernet
                }
            }
        };

        // Get applications for this department
        let dept_apps = Application::for_department(department);
        let num_apps = rng.gen_range(3..=dept_apps.len());
        let applications: Vec<Application> = dept_apps
            .choose_multiple(&mut rng, num_apps)
            .copied()
            .collect();

        // 5% of devices are anomalous
        let is_anomalous = rng.gen_bool(0.05);

        // Base performance varies per device (simulates hardware differences)
        let base_cpu = rng.gen_range(15.0..35.0);
        let base_memory = rng.gen_range(40.0..60.0);
        let base_disk = rng.gen_range(30.0..70.0);

        devices.push(Device {
            id: format!("device-{:04}", i + 1),
            device_type,
            os_version,
            department,
            location,
            connection_type,
            applications,
            is_anomalous,
            base_cpu,
            base_memory,
            base_disk,
        });
    }

    devices
}

/// Selects an item based on weights.
fn weighted_choice<T: Copy>(rng: &mut impl Rng, items: &[(T, u32)]) -> T {
    let total: u32 = items.iter().map(|(_, w)| w).sum();
    let mut choice = rng.gen_range(0..total);

    for (item, weight) in items {
        if choice < *weight {
            return *item;
        }
        choice -= weight;
    }

    items[0].0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_fleet() {
        let fleet = generate_fleet(100);
        assert_eq!(fleet.len(), 100);

        // Check device IDs are unique and properly formatted
        for (i, device) in fleet.iter().enumerate() {
            assert_eq!(device.id, format!("device-{:04}", i + 1));
        }

        // Check that roughly 5% are anomalous
        let anomalous_count = fleet.iter().filter(|d| d.is_anomalous).count();
        assert!(anomalous_count > 0 && anomalous_count < 20); // Allow some variance
    }

    #[test]
    fn test_device_type_battery() {
        assert!(DeviceType::Laptop.has_battery());
        assert!(!DeviceType::Desktop.has_battery());
        assert!(!DeviceType::Virtual.has_battery());
    }
}
