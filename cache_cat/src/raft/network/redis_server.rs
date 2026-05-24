use crate::protocol::command::{Client, CommandFactory, CommandResult};
use crate::protocol::resp::Parser;
use crate::raft::network::pub_sub::PubSub;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::raft_types::CacheCatApp;
use bytes::{Buf, BytesMut};
use futures::{FutureExt, SinkExt, StreamExt, future::BoxFuture, stream::FuturesOrdered};
use parking_lot::Mutex;
use std::io::Result as IoResult;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::{debug, error, info, warn};

#[derive(Clone)]
pub struct RedisServer {
    pub(crate) app: Arc<CacheCatApp>,
    pub redis_addr: String,
    pub cmd_factory: Arc<CommandFactory>,
    pub broadcast: Arc<PubSub>,
}

pub struct RespCodec;

impl Decoder for RespCodec {
    type Item = Value;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match Parser::parse(src) {
            Some((value, consumed)) => {
                src.advance(consumed);
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }
}

impl Encoder<Value> for RespCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Value, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(&item.encode());
        Ok(())
    }
}

impl RedisServer {
    pub fn new(
        app: Arc<CacheCatApp>,
        redis_addr: String,
        cmd_factory: Arc<CommandFactory>,
    ) -> Self {
        Self {
            app,
            redis_addr,
            cmd_factory,
            broadcast: Arc::new(PubSub::new()),
        }
    }

    async fn handle_connection_pipeline(
        self: Arc<Self>,
        stream: TcpStream,
        peer_addr: SocketAddr,
        client_id: u64,
    ) -> IoResult<()> {
        stream.set_nodelay(true)?;
        let framed = Framed::new(stream, RespCodec);
        let (mut writer, mut reader) = framed.split();
        let mut client = Client {
            db_number: 0,
            transaction_queue: None,
            id: client_id,
        };
        while let Some(frame_result) = reader.next().await {
            match frame_result {
                Ok(value) => {
                    debug!("Received command from {}: {:?}", peer_addr, value);
                    match self.cmd_factory.execute(&mut client, value, &self).await {
                        CommandResult::Immediate(resp) => {
                            if let Err(e) = writer.send(resp).await {
                                warn!("Failed to send response to {}: {}", peer_addr, e);
                                break;
                            }
                        }
                        CommandResult::Subscribe(res, mut stream) => {
                            // 发送订阅确认响应
                            if let Err(e) = writer.send(res).await {
                                warn!("Failed to send response to {}: {}", peer_addr, e);
                                break;
                            }

                            // 进入订阅模式循环
                            loop {
                                tokio::select! {
                                    // 接收来自订阅流的消息
                                    stream_result = stream.changed() => {
                                        match stream_result {
                                            Ok(_) => {
                                                let value = { stream.borrow().clone() };
                                                match value {
                                                    None => break, // 所有订阅都取消了
                                                    Some(v) => {
                                                        if let Err(e) = writer.send(v).await {
                                                            warn!("Failed to send response to {}: {}", peer_addr, e);
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                            Err(_) => break, // 流发送端关闭
                                        }
                                    }

                                    // 继续处理客户端命令
                                    frame_result = reader.next() => {
                                        match frame_result {
                                            Some(Ok(value)) => {
                                                debug!("Received command in subscribe mode from {}: {:?}", peer_addr, value);

                                                // 提取命令名
                                                let cmd_name = match &value {
                                                    Value::Array(Some(items)) if !items.is_empty() => {
                                                        match &items[0] {
                                                            Value::BulkString(Some(data)) => {
                                                                String::from_utf8_lossy(data).to_uppercase()
                                                            }
                                                            Value::SimpleString(s) => s.to_uppercase(),
                                                            _ => {
                                                                let _ = writer.send(Value::error("invalid command format")).await;
                                                                continue;
                                                            }
                                                        }
                                                    }
                                                    _ => {
                                                        let _ = writer.send(Value::error("ERR failed to parse command")).await;
                                                        continue;
                                                    }
                                                };

                                                // 在订阅模式下只允许特定命令
                                                match cmd_name.as_str() {
                                                    "SUBSCRIBE" | "PSUBSCRIBE" => {
                                                        // 允许添加新的订阅
                                                        match self.cmd_factory.execute(&mut client, value, &self).await {
                                                            CommandResult::Subscribe(resp, new_stream) => {
                                                                if let Err(e) = writer.send(resp).await {
                                                                    warn!("Failed to send subscribe response: {}", e);
                                                                    break;
                                                                }
                                                                // 更新流为新流
                                                                stream = new_stream;
                                                            }
                                                            CommandResult::Immediate(resp) => {
                                                                if let Err(e) = writer.send(resp).await {
                                                                    warn!("Failed to send response: {}", e);
                                                                    break;
                                                                }
                                                            }
                                                            _ => {
                                                                let _ = writer.send(Value::error("ERR invalid response type")).await;
                                                            }
                                                        }
                                                    }

                                                    "UNSUBSCRIBE" | "PUNSUBSCRIBE" => {
                                                        // 允许取消订阅
                                                        match self.cmd_factory.execute(&mut client, value, &self).await {
                                                            CommandResult::Subscribe(resp, new_stream) => {
                                                                if let Err(e) = writer.send(resp).await {
                                                                    warn!("Failed to send unsubscribe response: {}", e);
                                                                    break;
                                                                }
                                                                // 更新流
                                                                stream = new_stream;
                                                                // 检查是否所有订阅都已取消
                                                                if stream.borrow().is_none() {
                                                                    break; // 退出订阅模式
                                                                }
                                                            }
                                                            CommandResult::Immediate(resp) => {
                                                                if let Err(e) = writer.send(resp).await {
                                                                    warn!("Failed to send response: {}", e);
                                                                    break;
                                                                }
                                                            }
                                                            _ => {
                                                                let _ = writer.send(Value::error("ERR invalid response type")).await;
                                                            }
                                                        }
                                                    }

                                                    "PING" => {
                                                        // 在订阅模式下支持 PING
                                                        match self.cmd_factory.execute(&mut client, value, &self).await {
                                                            CommandResult::Immediate(resp) => {
                                                                if let Err(e) = writer.send(resp).await {
                                                                    warn!("Failed to send pong response: {}", e);
                                                                    break;
                                                                }
                                                            }
                                                            _ => {
                                                                let _ = writer.send(Value::error("ERR invalid PING response")).await;
                                                            }
                                                        }
                                                    }

                                                    "QUIT" => {
                                                        // QUIT 命令退出
                                                        let _ = writer.send(Value::SimpleString("OK".to_string())).await;
                                                        break; // 退出订阅模式循环
                                                    }

                                                    "RESET" => {
                                                        // 可选：支持 RESET 命令
                                                        let _ = writer.send(Value::SimpleString("RESET".to_string())).await;
                                                        break; // 退出订阅模式
                                                    }

                                                    _ => {
                                                        // 其他命令在订阅模式下被拒绝
                                                        let error_msg = format!(
                                                            "ERR Can't execute '{}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context",
                                                            cmd_name
                                                        );
                                                        if let Err(e) = writer.send(Value::error(&error_msg)).await {
                                                            warn!("Failed to send error response: {}", e);
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                            Some(Err(e)) => {
                                                error!("Protocol error from {}: {}", peer_addr, e);
                                                break;
                                            }
                                            None => {
                                                // 客户端断开连接
                                                info!("Client {} disconnected during subscribe mode", peer_addr);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Protocol error from {}: {}", peer_addr, e);
                    break;
                }
            }
        }
        info!("Connection handler ended for {}", peer_addr);
        Ok(())
    }

    pub async fn start_redis_server(self: Arc<Self>) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.redis_addr.clone()).await?;
        let mut client_id: u64 = 0;
        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    info!("New connection accepted from {}", peer_addr);
                    let server = Arc::clone(&self);
                    client_id = client_id + 1;
                    tokio::spawn(async move {
                        if let Err(e) = server
                            .handle_connection_pipeline(stream, peer_addr, client_id)
                            .await
                        {
                            error!("Error handling connection from {}: {}", peer_addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }
}
