mod server;
mod parsing;

#[tokio::main]
async fn main() {
    server::run().await;
}
