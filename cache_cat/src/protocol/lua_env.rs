use crate::protocol::raft_command::RaftCommandFactory;
use crate::raft::types::core::response_value::Value as RaftValue;
use mlua::prelude::LuaError;
use mlua::{Lua, Value, Variadic};

struct LuaEnv {
    lua: Lua,
}

struct RedisHandler {
    raft_command: RaftCommandFactory,
}

impl RedisHandler {
    fn new() -> RedisHandler {
        RedisHandler {
            raft_command: RaftCommandFactory::init_lua(),
        }
    }

    fn call(&self, lua: &Lua, args: Variadic<String>) -> mlua::Result<Value> {
        if args.is_empty() {
            return Err(LuaError::external(
                "redis.call requires at least one argument",
            ));
        }
        let mut vec = Vec::new();
        for param in args {
            vec.push(RaftValue::SimpleString(param));
        }
        let x = self.raft_command.parse_request(&vec);
        todo!()
    }
}

impl LuaEnv {
    fn new() -> mlua::Result<LuaEnv> {
        let lua = Lua::new();

        // 沙箱设置
        let globals = lua.globals();
        globals.set("os", Value::Nil)?;
        globals.set("io", Value::Nil)?;
        globals.set("package", Value::Nil)?;
        globals.set("require", Value::Nil)?;
        globals.set("dofile", Value::Nil)?;
        globals.set("loadfile", Value::Nil)?;

        let handler = RedisHandler::new();
        let redis_api = lua.create_table()?;

        redis_api.set(
            "call",
            lua.create_function(move |lua_ctx, args: Variadic<String>| {
                handler.call(lua_ctx, args)
            })?,
        )?;

        globals.set("redis", redis_api)?;

        Ok(LuaEnv { lua })
    }

    fn exec_lua(&self, cmd: &str) -> mlua::Result<Value> {
        let result: Value = self.lua.load(cmd).eval()?;
        Ok(result)
    }
}
