use crate::error::ProtocolError;
use crate::protocol::hash::hget::HGetCommand;
use crate::protocol::hash::hincrby::HIncrByCommand;
use crate::protocol::hash::hset::HSetCommand;
use crate::protocol::key::del::DelCommand;
use crate::protocol::key::exists::ExistsCommand;
use crate::protocol::key::expire::ExpireCommand;
use crate::protocol::key::persist::PersistCommand;
use crate::protocol::key::rename::RenameCommand;
use crate::protocol::list::lpush::LPushCommand;
use crate::protocol::list::lrange::LRangeCommand;
use crate::protocol::set::sadd::SAddCommand;
use crate::protocol::string::append::AppendCommand;
use crate::protocol::string::get::GetCommand;
use crate::protocol::string::incr::IncrCommand;
use crate::protocol::string::incrby::IncrByCommand;
use crate::protocol::string::mget::MgetCommand;
use crate::protocol::string::mset::MsetCommand;
use crate::protocol::string::set::SetCommand;
use crate::protocol::zset::zadd::ZAddCommand;
use crate::protocol::zset::zrange::ZRangeCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::Operation;
use std::collections::HashMap;

pub trait RaftCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError>;
}

/// Command factory for creating and executing commands
pub struct RaftCommandFactory {
    commands: HashMap<String, Box<dyn RaftCommand>>,
}

impl RaftCommandFactory {
    /// Create a new empty command factory
    fn new() -> Self {
        Self {
            commands: HashMap::new(),
        }
    }

    /// Register a command with given name
    fn register<C: RaftCommand + 'static>(&mut self, name: impl Into<String>, cmd: C) {
        self.commands.insert(name.into(), Box::new(cmd));
    }

    /// Initialize the command factory with all supported commands
    pub fn init_lua() -> Self {
        let mut factory = Self::new();
        // Register connection commands
        factory.register("GET", GetCommand);
        factory.register("SET", SetCommand);
        factory.register("DEL", DelCommand);
        factory.register("INCR", IncrCommand);
        factory.register("INCRBY", IncrByCommand);
        factory.register("MSET", MsetCommand);
        factory.register("MGET", MgetCommand);
        factory.register("LPUSH", LPushCommand);
        factory.register("LRANGE", LRangeCommand);
        factory.register("EXPIRE", ExpireCommand);
        factory.register("APPEND", AppendCommand);
        factory.register("HSET", HSetCommand);
        factory.register("HGET", HGetCommand);
        factory.register("ZADD", ZAddCommand);
        factory.register("ZRANGE", ZRangeCommand);
        factory.register("SADD", SAddCommand);
        factory.register("HINCRBY", HIncrByCommand);
        factory.register("EXISTS", ExistsCommand);
        factory.register("PERSIST", PersistCommand);
        factory.register("RENAME", RenameCommand);
        factory
    }
}
