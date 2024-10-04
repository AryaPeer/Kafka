use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::convert::TryFrom;
use std::fmt;

#[derive(Debug, Clone)]
pub struct InvalidEnumVariant;

impl fmt::Display for InvalidEnumVariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Invalid enum variant")
    }
}

impl std::error::Error for InvalidEnumVariant {}

macro_rules! int_enum {
    ($name:ident, $type:ty, $($variant:ident = $value:literal),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name {
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

pub const VERSIONS: &'static [(KafkaApiKey, u16, u16)] = &[
    (KafkaApiKey::Fetch, 16, 16),
    (KafkaApiKey::ApiVersions, 4, 4),
];

pub fn process_message(message_buf: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut message = Bytes::copy_from_slice(message_buf);

    for (i, byte) in message.iter().enumerate() {
        print!("{:02x} ", byte);
        if (i + 1) % 16 == 0 {
            println!();
        }
    }

    let request_api_key = KafkaApiKey::try_from(message.get_u16())?;
    let request_api_version = message.get_u16();
    let correlation_id = message.get_u32();

    if !VERSIONS.iter().any(|&(key, min, max)| {
        key == request_api_key && request_api_version >= min && request_api_version <= max
    }) {
        let mut response = BytesMut::new();
        response.put_u32(correlation_id);
        response.put_u16(KafkaError::UnsupportedVersion as u16);
        let length = response.len() as u32;
        let mut full_response = Vec::new();
        full_response.extend_from_slice(&length.to_be_bytes());
        full_response.extend_from_slice(&response);
        return Ok(full_response);
    }

    let response = match request_api_key {
        KafkaApiKey::Fetch => process_fetch_request(&mut message, correlation_id)?,
        KafkaApiKey::ApiVersions => process_api_versions_request(correlation_id)?,
    };

    Ok(response)
}

fn process_fetch_request(message: &mut Bytes, correlation_id: u32) -> anyhow::Result<Vec<u8>> {
    let client_id_len = message.get_u16() as usize;
    message.advance(client_id_len);
    message.advance(1);
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
            message.advance(1);
        }
        message.advance(1);
        topics.push(topic_id);
    }

    let _forgotten_topics_data_count = message.get_u8().saturating_sub(1);
    let _rack_id_len = message.get_u8().saturating_sub(1);
    message.advance(1); 

    let mut response = BytesMut::new();
    response.put_u32(correlation_id); 
    response.put_u8(0);
    response.put_u32(0);
    response.put_u16(KafkaError::NoError as u16); 
    response.put_u32(0);
    response.put_u8(topics.len() as u8 + 1); 

    for topic_id in topics {
        response.put_u128(topic_id);
        response.put_u8(2);
        response.put_u32(0);
        response.put_u16(KafkaError::UnknownTopic as u16);
        response.put_u64(0);
        response.put_u64(0);
        response.put_u64(0);
        response.put_u8(0);
        response.put_u32(0);
        response.put_u8(1);
        response.put_u8(0);
        response.put_u8(0);
    }

    response.put_u8(0); 

    let length = response.len() as u32;
    let mut full_response = Vec::new();
    full_response.extend_from_slice(&length.to_be_bytes());
    full_response.extend_from_slice(&response);

    Ok(full_response)
}

fn process_api_versions_request(correlation_id: u32) -> anyhow::Result<Vec<u8>> {
    let mut response = BytesMut::new();
    response.put_u32(correlation_id); 
    response.put_u16(KafkaError::NoError as u16); 
    response.put_u8(VERSIONS.len() as u8 + 1); 

    for &(key, min, max) in VERSIONS {
        response.put_u16(key as u16);
        response.put_u16(min);
        response.put_u16(max);
        response.put_u8(0); 
    }

    response.put_u32(0); 
    response.put_u8(0); 

    let length = response.len() as u32;
    let mut full_response = Vec::new();
    full_response.extend_from_slice(&length.to_be_bytes());
    full_response.extend_from_slice(&response);

    Ok(full_response)
}