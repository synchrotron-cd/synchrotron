use std::path::PathBuf;

pub struct ServerConfig {
    pub listen_addr: String,
    pub db_path: PathBuf,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:8484".to_string(),
            db_path: PathBuf::from("synchrotron.db"),
        }
    }
}
