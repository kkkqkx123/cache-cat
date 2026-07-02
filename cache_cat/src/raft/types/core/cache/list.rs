use crate::protocol::list::lpop::LPopReq;
use crate::protocol::list::lpush::LPushReq;
use crate::protocol::list::rpop::RPopReq;
use crate::protocol::list::rpush::RPushReq;
use crate::raft::types::core::mocha::mocha::{MyCache, Update};
use crate::raft::types::core::response_value::Value;

impl MyCache {


    pub fn r_push(&self, param: RPushReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    pub fn l_push(&self, param: LPushReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
    pub fn l_pop(&self, param: LPopReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    pub fn r_pop(&self, param: RPopReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
}
