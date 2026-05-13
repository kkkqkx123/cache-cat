use crate::protocol::raft_command::RaftCommandFactory;
use crate::raft::store::statemachine::StateMachineStore;
use crate::raft::types::core::moka::moka::{Database, MyCache, Update};
use crate::raft::types::core::moka::request_handler::do_request;
use crate::raft::types::core::response_value::Value as RaftValue;
use mlua::prelude::LuaError;
use mlua::{Lua, Value, Variadic};

use serde::__private228::de::IdentifierDeserializer;

pub struct LuaEnv {
    lua: Lua,
    raft_command: RaftCommandFactory, // 直接持有
}

impl LuaEnv {
    pub fn new() -> mlua::Result<LuaEnv> {
        let lua = Lua::new();
        // 沙箱设置
        let globals = lua.globals();
        globals.set("os", Value::Nil)?;
        globals.set("io", Value::Nil)?;
        globals.set("package", Value::Nil)?;
        globals.set("require", Value::Nil)?;
        globals.set("dofile", Value::Nil)?;
        globals.set("loadfile", Value::Nil)?;

        Ok(LuaEnv {
            lua,
            raft_command: RaftCommandFactory::init_lua(),
        })
    }

    pub fn exec_lua(&self, cache: &MyCache, cmd: &str, update: &mut Update) -> mlua::Result<Value> {
        // scope 允许借用一个 &mut update
        self.lua.scope(|scope| -> mlua::Result<Value> {
            // 创建临时的 redis.call 闭包，可以捕获 &mut update 和 &self.raft_command
            let redis_call = scope.create_function_mut(|lua_ctx, args: Variadic<String>| {
                if args.is_empty() {
                    return Err(LuaError::external(
                        "redis.call requires at least one argument",
                    ));
                }
                // 1. 构建参数
                let mut vec = Vec::new();
                for param in args {
                    vec.push(RaftValue::SimpleString(param));
                }

                // 2. 解析命令
                let operation = self
                    .raft_command
                    .parse_request(&vec)
                    .map_err(|e| LuaError::external(e))?;
                let value = do_request(cache, operation, update);
                let str = match value {
                    RaftValue::BulkString(value) => {
                        String::from_utf8_lossy(&*value.unwrap()).to_string()
                    }
                    RaftValue::SimpleString(value) => value,
                    _ => String::from(""),
                };
                Ok(str) // 按需要返回值
            })?;

            // 注入临时的 redis 表
            let redis_table = self.lua.create_table()?;
            redis_table.set("call", redis_call)?;
            self.lua.globals().set("redis", redis_table)?;

            // 执行脚本
            self.lua.load(cmd).eval()
        })
    }
}
