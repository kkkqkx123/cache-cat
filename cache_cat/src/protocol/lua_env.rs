use std::sync::Arc;
use mlua::{Lua, Value, Variadic};
use mlua::prelude::LuaError;
use moka::sync::Cache;

struct LuaEnv {
    lua: Lua,
    redis_handler: RedisHandler,
}

#[derive(Clone)]
struct RedisHandler {
    cache: Arc<Cache<String, String>>,
}

impl RedisHandler {
    fn new() -> Self {
        RedisHandler {
            cache: Arc::new(
                Cache::builder()
                    .max_capacity(10_000)
                    .build()
            ),
        }
    }

    fn call(&self, lua: &Lua, args: Variadic<String>) -> mlua::Result<Value> {
        if args.is_empty() {
            return Err(LuaError::external("redis.call requires at least one argument"));
        }

        let command = args[0].to_uppercase();

        match command.as_str() {
            "GET" => self.handle_get(lua, &args),
            "SET" => self.handle_set(lua, &args),
            "DEL" => self.handle_del(lua, &args),
            "EXISTS" => self.handle_exists(lua, &args),
            _ => Err(LuaError::external(format!(
                "Unknown Redis command: {}", command
            ))),
        }
    }

    fn handle_get(&self, lua: &Lua, args: &[String]) -> mlua::Result<Value> {
        if args.len() != 2 {
            return Err(LuaError::external("ERR wrong number of arguments for 'GET' command"));
        }

        let key = &args[1];
        match self.cache.get(key) {
            Some(value) => Ok(Value::String(lua.create_string(&value)?)),
            None => Ok(Value::Nil),
        }
    }

    fn handle_set(&self, lua: &Lua, args: &[String]) -> mlua::Result<Value> {
        if args.len() < 3 {
            return Err(LuaError::external(
                "ERR wrong number of arguments for 'SET' command"
            ));
        }

        let key = &args[1];
        let value = &args[2];

        self.cache.insert(key.clone(), value.clone());
        Ok(Value::String(lua.create_string("OK")?))
    }

    fn handle_del(&self, _lua: &Lua, args: &[String]) -> mlua::Result<Value> {
        let mut count = 0i64;
        for key in &args[1..] {
            self.cache.invalidate(key);
            count += 1;
        }
        Ok(Value::Integer(count))
    }

    fn handle_exists(&self, _lua: &Lua, args: &[String]) -> mlua::Result<Value> {
        let mut count = 0i64;
        for key in &args[1..] {
            if self.cache.contains_key(key) {
                count += 1;
            }
        }
        Ok(Value::Integer(count))
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

        let handler_clone = handler.clone();
        redis_api.set(
            "call",
            lua.create_function(move |lua_ctx, args: Variadic<String>| {
                handler_clone.call(lua_ctx, args)
            })?,
        )?;

        globals.set("redis", redis_api)?;

        Ok(LuaEnv {
            lua,
            redis_handler: handler,
        })
    }

    fn exec_lua(&self, cmd: &str) -> mlua::Result<Value> {
        let result: Value = self.lua.load(cmd).eval()?;
        Ok(result)
    }
}