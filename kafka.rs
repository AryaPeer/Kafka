#![allow(unused_imports)]

use bytes::BufMut;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn process_stream(connection: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut len_buffer = [0; 4];
    connection.read(&mut len_buffer)?;

    let mut key_buffer = [0; 2];
    connection.read(&mut key_buffer)?;
    let key_code = i16::from_be_bytes(key_buffer);

    let mut version_buffer = [0; 2];
    connection.read(&mut version_buffer)?;
    let version_code = i16::from_be_bytes(version_buffer);

    let mut id_buffer = [0; 4];
    connection.read(&mut id_buffer)?;

    let mut remaining_data = [0; 1024];
    connection.read(&mut remaining_data)?;

    let mut response_data: Vec<u8> = Vec::new();

    if key_code == 18 {
        process_api_versions(&mut response_data, version_code, &id_buffer);
    }

    let mut final_response = Vec::new();
    final_response.put_i32(response_data.len().try_into().unwrap());
    final_response.put(&response_data[..]);

    Ok(final_response)
}

fn process_api_versions(response_data: &mut Vec<u8>, version_code: i16, id_buffer: &[u8]) {
    let mut error_flag: i16 = 0;
    if !(0..=4).contains(&version_code) {
        error_flag = 35;
    }

    response_data.put(id_buffer);
    response_data.put(&error_flag.to_be_bytes()[..]); 
    response_data.put_i8(3);
    response_data.put_i16(1);
    response_data.put_i16(0);
    response_data.put_i16(16);
    response_data.put_i8(0);
    response_data.put_i16(18); 
    response_data.put_i16(0);
    response_data.put_i16(4);
    response_data.put_i8(0);
    response_data.put_i8(0); 
    response_data.put_i32(0);
}

fn main() {
    println!("Logs will appear here!");

    let server = TcpListener::bind("127.0.0.1:9092").expect("Unable to bind to address");

    for connection in server.incoming() {
        match connection {
            Ok(mut stream) => {
                thread::spawn(move || {
                    loop {
                        let mut peek_buf = [0; 1];
                        if stream.peek(&mut peek_buf).is_err() {
                            break;
                        }

                        println!("New connection established");

                        let reply = process_stream(&mut stream).unwrap_or_else(|e| {
                            println!("Error reading stream: {}", e);
                            Vec::new()
                        });

                        println!("Response: {:?}", reply);

                        if let Err(e) = stream.write(&reply) {
                            println!("Write error: {}", e);
                        }
                    }
                });
            }
            Err(e) => {
                println!("Connection error: {}", e);
            }
        }
    }
}