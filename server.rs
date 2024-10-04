use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::request_handler::handle_request;

pub mod model;
pub mod responses;

pub async fn handle_stream(mut stream: TcpStream) -> anyhow::Result<()> {
    println!("Accepted new connection from {}", stream.peer_addr().unwrap());

    loop {
        let mut length_buf = [0; 4];
        stream.read_exact(&mut length_buf).await?;
        let length = u32::from_be_bytes(length_buf) as usize;
        let mut message_buf = vec![0; length];
        stream.read_exact(&mut message_buf).await?;
        
        // Pass the message buffer as a Vec<u8>
        let response = handle_request(message_buf).await?;
        
        // Send the response back
        stream.write_all(&response).await?;
    }
}
