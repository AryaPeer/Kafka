use bytes::{Bytes, BytesMut, BufMut};

pub fn build_response(correlation_id: u32, error_code: u16) -> Bytes {
    let mut response = BytesMut::new();
    response.put_u32(correlation_id); // Correlation ID
    response.put_u16(error_code); // Error Code
    response.freeze()
}