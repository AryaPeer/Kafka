use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::parsing;

pub async fn run() {
    let listener = TcpListener::bind("127.0.0.1:9092").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(async move {
                    if let Err(e) = handle_stream(stream).await {
                        eprintln!("Error handling stream: {:?}", e);
                    }
                });
            }
            Err(e) => eprintln!("Failed to accept connection: {:?}", e),
        }
    }
}

async fn handle_stream(mut stream: TcpStream) -> anyhow::Result<()> {
    println!("Accepted new connection from {}", stream.peer_addr()?);

    loop {
        let mut length_buf = [0; 4];
        if stream.read_exact(&mut length_buf).await.is_err() {
            break;
        }
        let length = u32::from_be_bytes(length_buf) as usize;
        let mut message_buf = vec![0; length];
        stream.read_exact(&mut message_buf).await?;

        let response = parsing::process_message(&message_buf)?;

        stream.write_all(&response).await?;
    }

    Ok(())
}
