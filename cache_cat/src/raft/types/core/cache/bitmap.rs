use crate::protocol::bitmap::getbit::GetBitParams;
use crate::protocol::bitmap::setbit::SetBitReq;
use crate::raft::types::core::mocha::mocha::{MyCache, Update};
use crate::raft::types::core::response_value::Value;

impl MyCache {
    pub fn get_bit(&self, param: GetBitParams, db_number: u16, read_clock: Option<u64>) -> Value {
        self.execute_read(param, db_number, read_clock)
    }

    pub fn set_bit(&self, param: SetBitReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
}
