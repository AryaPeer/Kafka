use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug)]
struct InvalidEnumVariant;

macro_rules! int_enum {
    ($name:ident, $type:ty, $($variant:ident = $value:literal),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum $name {
            $($variant = $value),+
        }

        impl TryFrom<$type> for $name {
            type Error = InvalidEnumVariant;

            fn try_from(value: $type) -> Result<Self, Self::Error> {
                match value {
                    $($value => Ok($name::$variant),)*
                    _ => Err(InvalidEnumVariant),
                }
            }
        }
    }
}

int_enum!(KafkaApiKey, u16, Fetch = 1, ApiVersions = 18);
int_enum!(KafkaError, u16, NoError = 0, UnsupportedVersion = 35, UnknownTopic = 100);

const VERSIONS: &'static [(KafkaApiKey, u16, u16)] = &[
    (KafkaApiKey::Fetch, 16, 16),
    (KafkaApiKey::ApiVersions, 4, 4),
];

async fn handle_stream(mut stream: tokio::net::TcpStream) -> anyhow::Result<()> {
    println!("accepted new connection from {}", stream.peer_addr().unwrap());

    loop {
        let mut length_buf = [0; 4];
        stream.read_exact(&mut length_buf).await?;
        let length = u32::from_be_bytes(length_buf) as usize;
        let mut message_buf = vec![0; length];
        stream.read_exact(&mut message_buf).await?;
        let mut message = Bytes::from(message_buf);

        for (i, byte) in message.iter().enumerate() {
            print!("{byte:02x} ");
            if (i + 1) % 16 == 0 {
                println!();
            }
        }

        let request_api_key = KafkaApiKey::try_from(message.get_u16()).unwrap();
        let request_api_version = message.get_u16();
        let correlation_id = message.get_u32();

        // Check supported versions
        match VERSIONS.iter().find(|&&(key, _, _)| key == request_api_key) {
            Some(&(_, min, max)) if request_api_version >= min && request_api_version <= max => (),
            _ => {
                let mut response = BytesMut::new();
                response.put_u32(correlation_id);
                response.put_u16(KafkaError::UnsupportedVersion as u16);
                let length = response.len() as u32;
                stream.write_all(&length.to_be_bytes()).await?;
                stream.write_all(&response).await?;
                continue;
            }
        }

        let mut response = BytesMut::new();

        match request_api_key {
            KafkaApiKey::Fetch => {
                let client_id_len = message.get_u16() as usize;
                message.advance(client_id_len);
                message.advance(1); // TAG_BUFFER
                let _max_wait_ms = message.get_u32();
                let _min_bytes = message.get_u32();
                let _max_bytes = message.get_u32();
                let _isolation_level = message.get_u8();
                let _session_id = message.get_u32();
                let _session_epoch = message.get_u32();
                let topic_count = message.get_u8().saturating_sub(1);
                let mut topics = Vec::new();

                for _ in 0..topic_count {
                    let topic_id = message.get_u128();
                    let partition_count = message.get_u8().saturating_sub(1);

                    for _ in 0..partition_count {
                        let _partition = message.get_u32();
                        let _current_leader_epoch = message.get_u32();
                        let _fetch_offset = message.get_u64();
                        let _last_fetched_epoch = message.get_u32();
                        let _log_start_offset = message.get_u64();
                        let _partition_max_bytes = message.get_u32();
                        message.advance(1); // TAG_BUFFER
                    }
                    message.advance(1); // TAG_BUFFER
                    topics.push(topic_id);
                }

                let _forgotten_topics_data_count = message.get_u8().saturating_sub(1);
                let _rack_id_len = message.get_u8() - 1;
                message.advance(1); // TAG_BUFFER

                response.put_u32(correlation_id); // Header
                response.put_u8(0);
                response.put_u32(0); // Throttle Time
                response.put_u16(KafkaError::NoError as u16); // Error Code
                response.put_u32(0); // Session ID
                response.put_u8(topics.len() as u8 + 1); // Responses

                for topic_id in topics {
                    response.put_u128(topic_id);
                    response.put_u8(2); // Partitions
                    response.put_u32(0); // Index
                    response.put_u16(KafkaError::UnknownTopic as u16);
                    response.put_u64(0);
                    response.put_u64(0);
                    response.put_u64(0);
                    response.put_u8(0);
                    response.put_u32(0);
                    response.put_u8(1);
                    response.put_u8(0); // TAG_BUFFER
                    response.put_u8(0); // TAG_BUFFER
                }

                response.put_u8(0); // TAG_BUFFER
            }

            KafkaApiKey::ApiVersions => {
                response.put_u32(correlation_id); // Header
                response.put_u16(KafkaError::NoError as u16); // Error Code
                response.put_u8(VERSIONS.len() as u8 + 1); // API Keys

                for &(key, min, max) in VERSIONS {
                    response.put_u16(key as u16);
                    response.put_u16(min);
                    response.put_u16(max);
                    response.put_u8(0); // TAG_BUFFER
                }

                response.put_u32(0); // Throttle Time
                response.put_u8(0); // TAG_BUFFER
            }
        }

        let length = response.len() as u32;
        stream.write_all(&length.to_be_bytes()).await?;
        stream.write_all(&response).await?;
    }
}

#[tokio::main]
async fn main() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:9092")
        .await
        .unwrap();

    loop {
        if let Ok((stream, _addr)) = listener.accept().await {
            tokio::spawn(async {
                handle_stream(stream).await.unwrap();
            });
        };
    }
}
