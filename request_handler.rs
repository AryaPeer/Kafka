use bytes::{Buf, Bytes, BytesMut, BufMut}; // Added BufMut here
use crate::server::model::{KafkaApiKey, KafkaError};
use crate::server::responses::build_response;

pub async fn handle_request(message_buf: Vec<u8>) -> anyhow::Result<Bytes> {
    let mut message = Bytes::from(message_buf);

    let request_api_key = KafkaApiKey::try_from(message.get_u16()).unwrap();
    let _request_api_version = message.get_u16(); // Suppressed unused variable warning
    let correlation_id = message.get_u32();

    // Logic for processing the request and generating a response
    let response = match request_api_key {
        KafkaApiKey::Fetch => {
            // Fetch request handling
            handle_fetch_request(message, correlation_id).await
        },
        KafkaApiKey::ApiVersions => {
            // ApiVersions request handling
            handle_api_versions_request(correlation_id).await
        },
    };

    Ok(response)
}

// Example handlers for requests
async fn handle_fetch_request(mut message: Bytes, correlation_id: u32) -> Bytes {
    println!("Handling Fetch request with correlation ID: {}", correlation_id); // Added logging

    let mut topics = Vec::new();
    let topic_count = message.get_u32();
    for _ in 0..topic_count {
        let topic_id = message.get_u128();
        let partition_count = message.get_u8().saturating_sub(1);

        for _ in 0..partition_count {
            let _partition = message.get_u32();
            // Advance through the rest of the partition data (simplified)
            message.advance(25); // Adjust this if more fields exist
        }
        topics.push(topic_id);
    }

    let mut response = BytesMut::new();
    response.put_u32(correlation_id); // Correlation ID
    response.put_u16(KafkaError::NoError as u16); // Error Code
    response.put_u32(0); // Throttle Time
    response.put_u32(0); // Session ID
    response.put_u8(1); // Number of topics in response

    for topic_id in topics {
        response.put_u128(topic_id); // Topic ID
        response.put_u8(1); // Number of partitions
        response.put_u32(0); // Partition Index
        response.put_u16(KafkaError::UnknownTopic as u16); // Unknown topic error
        response.put_u64(0); // Fetch offset
        response.put_u64(0); // High watermark
        response.put_u64(0); // Log start offset
        response.put_u32(0); // Partition max bytes
        response.put_u8(0); // TAG_BUFFER for partitions
    }

    response.put_u8(0); // TAG_BUFFER for topics
    response.freeze()
}

async fn handle_api_versions_request(correlation_id: u32) -> Bytes {
    // Generate ApiVersions response
    let mut response = BytesMut::new();
    response.put_u32(correlation_id); // Correlation ID
    response.put_u16(KafkaError::NoError as u16); // Error code
    response.freeze()
}
