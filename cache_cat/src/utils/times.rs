use std::time::SystemTime;
use std::time::UNIX_EPOCH;



/// Get the current timestamp in milliseconds
pub fn now_ms() -> u64 {
  SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_millis() as u64
}
