use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::Command;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;

pub struct SelectCommand;

#[async_trait]
impl Command for SelectCommand {
    async fn execute(
        &self,
        db_number: &mut u16,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if items.len() > 2 {
            return Err(ProtocolError::WrongArgCount("select").into());
        }

        let mut num: u16 = 0;
        if items.len() == 2 {
            match &items[1] {
                Value::Integer(s) => num = *s as u16,
                Value::SimpleString(s) => {
                    num = s.parse::<u16>().map_err(|_| ProtocolError::SyntaxError)?;
                }
                Value::BulkString(Some(bytes)) => {
                    let num = std::str::from_utf8(&bytes)
                        .map_err(|_| ProtocolError::WrongArgCount("select"))?
                        .parse::<u16>()
                        .map_err(|_| ProtocolError::WrongArgCount("select"))?;
                }
                _ => return Err(CacheCatError::from(ProtocolError::SyntaxError)),
            }
        }
        *db_number = num;
        Ok(Value::ok())
    }
}
