use crate::error::CacheCatError;
use crate::protocol::bitmap::getbit::GetBitCommand;
use crate::protocol::bitmap::setbit::SetBitCommand;
use crate::protocol::command::CommandResult::Immediate;
use crate::protocol::connection::bgsave::BgsaveCommand;
use crate::protocol::connection::echo::EchoCommand;
use crate::protocol::connection::ping::PingCommand;
use crate::protocol::connection::save::SaveCommand;
use crate::protocol::connection::select::SelectCommand;
use crate::protocol::connection::time::TimeCommand;
use crate::protocol::hash::hdel::HDelCommand;
use crate::protocol::hash::hget::HGetCommand;
use crate::protocol::hash::hincrby::HIncrByCommand;
use crate::protocol::hash::hmget::{HMGetCommand, HMGetParams};
use crate::protocol::hash::hset::HSetCommand;
use crate::protocol::key::del::DelCommand;
use crate::protocol::key::exists::ExistsCommand;
use crate::protocol::key::expire::ExpireCommand;
use crate::protocol::key::persist::PersistCommand;
use crate::protocol::key::rename::RenameCommand;
use crate::protocol::list::lpush::LPushCommand;
use crate::protocol::list::lrange::LRangeCommand;
use crate::protocol::lua::eval::EvalCommand;
use crate::protocol::lua::evalsha::EvalShaCommand;
use crate::protocol::lua::script::{ScriptCommand, ScriptParam};
use crate::protocol::pub_sub::publish::PublishCommand;
use crate::protocol::pub_sub::subscribe::SubscribeCommand;
use crate::protocol::pub_sub::unsubscribe::UnsubscribeCommand;
use crate::protocol::set::sadd::SAddCommand;
use crate::protocol::set::smembers::SMembersCommand;
use crate::protocol::set::srem::SRemCommand;
use crate::protocol::string::append::AppendCommand;
use crate::protocol::string::get::GetCommand;
use crate::protocol::string::incr::IncrCommand;
use crate::protocol::string::incrby::IncrByCommand;
use crate::protocol::string::mget::MgetCommand;
use crate::protocol::string::mset::MsetCommand;
use crate::protocol::string::set::SetCommand;
use crate::protocol::transaction::discard::DiscardCommand;
use crate::protocol::transaction::exec::ExecCommand;
use crate::protocol::transaction::multi::MultiCommand;
use crate::protocol::zset::zadd::ZAddCommand;
use crate::protocol::zset::zrange::ZRangeCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use std::collections::HashMap;
use tokio::sync::watch;
use tracing::warn;

pub enum CommandResult {
    Immediate(Value),
    Subscribe(Value, watch::Receiver<Option<Value>>),
}

#[async_trait]
pub trait Command: Send + Sync {
    /// Execute the command with given RESP items and server context
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError>;
}

#[async_trait]
pub trait BlockCommand: Send + Sync {
    /// Execute the command with given RESP items and server context
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<(Value, watch::Receiver<Option<Value>>), CacheCatError>;
}

#[derive(Debug)]
pub struct Client {
    pub id: u64,
    pub db_number: u16,
    pub transaction_queue: Option<Vec<Operation>>,
}

/// Command factory for creating and executing commands
pub struct CommandFactory {
    commands: HashMap<String, Box<dyn Command>>,
    block_commands: HashMap<String, Box<dyn BlockCommand>>,
}

impl CommandFactory {
    /// Create a new empty command factory
    fn new() -> Self {
        Self {
            commands: HashMap::new(),
            block_commands: HashMap::new(),
        }
    }

    /// Register a command with given name
    fn register<C: Command + 'static>(&mut self, name: impl Into<String>, cmd: C) {
        self.commands.insert(name.into(), Box::new(cmd));
    }
    fn register_block<C: BlockCommand + 'static>(&mut self, name: impl Into<String>, cmd: C) {
        self.block_commands.insert(name.into(), Box::new(cmd));
    }

    /// Initialize the command factory with all supported commands
    pub fn init() -> Self {
        let mut factory = Self::new();

        // Register connection commands
        factory.register("GET", GetCommand);
        factory.register("SET", SetCommand);
        factory.register("DEL", DelCommand);
        factory.register("PING", PingCommand);
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
        factory.register("BGSAVE", BgsaveCommand);
        factory.register("SAVE", SaveCommand);
        factory.register("SELECT", SelectCommand);
        factory.register("ECHO", EchoCommand);
        factory.register("EVAL", EvalCommand);
        factory.register("MULTI", MultiCommand);
        factory.register("DISCARD", DiscardCommand);
        factory.register("EXEC", ExecCommand);
        factory.register("SMEMBERS", SMembersCommand);
        factory.register("HMGET", HMGetCommand);
        factory.register("SCRIPT", ScriptCommand);
        factory.register("EVALSHA", EvalShaCommand);
        factory.register("HDEL", HDelCommand);
        factory.register("SREM", SRemCommand);
        factory.register("SETBIT", SetBitCommand);
        factory.register("GETBIT", GetBitCommand);
        factory.register("TIME", TimeCommand);
        factory.register("PUBLISH", PublishCommand);
        factory.register("UNSUBSCRIBE", UnsubscribeCommand);

        factory.register_block("SUBSCRIBE", SubscribeCommand);

        factory
    }

    /// Execute a RESP command on the given server
    pub async fn execute(
        &self,
        client: &mut Client,
        value: Value,
        server: &RedisServer,
    ) -> CommandResult {
        match value {
            Value::Array(Some(items)) if !items.is_empty() => {
                // Extract command name
                let cmd_name = match &items[0] {
                    Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
                    Value::SimpleString(s) => s.to_uppercase(),
                    _ => return Immediate(Value::error("invalid command format")),
                };
                // Find and execute command
                if let Some(cmd) = self.commands.get(&cmd_name) {
                    return match cmd.execute(client, &items, server).await {
                        Ok(v) => Immediate(v),
                        Err(e) => {
                            warn!("Command '{}' error: {}", cmd_name, e);
                            Immediate(e.into())
                        }
                    };
                }
                if let Some(cmd) = self.block_commands.get(&cmd_name) {
                    match cmd.execute(client, &items, server).await {
                        Ok(v) => {
                            return CommandResult::Subscribe(v.0, v.1);
                        }
                        Err(e) => {
                            warn!("Command '{}' error: {}", cmd_name, e);
                            return Immediate(e.into());
                        }
                    }
                };
                Immediate(Value::error("ERR unknown command"))
            }
            _ => Immediate(Value::error("ERR failed to parse command")),
        }
    }
}
