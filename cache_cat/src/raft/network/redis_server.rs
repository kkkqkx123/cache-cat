use crate::error::CacheCatError;
use crate::protocol::command::{Client, CommandFactory};
use crate::protocol::resp::Parser;
use crate::raft::application::pub_sub::PubSub;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::raft_types::CacheCatApp;
use bytes::{Buf, BytesMut};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::{error, info};

#[derive(Clone)]
pub struct RedisServer {
    pub(crate) app: Arc<CacheCatApp>,
    pub redis_addr: String,
    pub cmd_factory: Arc<CommandFactory>,
    pub broadcast: Arc<PubSub>,
}

pub struct RespCodec {
    proto_version: u8,
}

impl RespCodec {
    pub fn new() -> Self {
        Self { proto_version: 2 }
    }
    pub fn switch_resp2(&mut self){
        self.proto_version = 2;
    }
    pub fn switch_resp3(&mut self){
        self.proto_version = 3;
    }
}

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
        let mut buf = Vec::new();
        item.encode_to(self.proto_version, &mut buf);
        dst.extend_from_slice(&buf);
        Ok(())
    }
}

impl RedisServer {
    pub fn new(app: Arc<CacheCatApp>, redis_addr: String) -> Self {
        let cmd_factory = Arc::new(CommandFactory::init());
        let broadcast = app.pubsub.clone();
        Self {
            app,
            redis_addr,
            cmd_factory,
            broadcast,
        }
    }

    async fn handle_connection_pipeline(
        self: Arc<Self>,
        stream: TcpStream,
        peer_addr: SocketAddr,
        client_id: u64,
    ) -> Result<(), CacheCatError> {
        stream.set_nodelay(true)?;
        let framed = Framed::new(stream, RespCodec::new());
        let auth = self.app.config.password.is_none();
        let client = Client::new(client_id, framed, auth);
        self.cmd_factory.process_connection(&self, client).await?;
        self.app.pubsub.remove_client(client_id).await;
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
