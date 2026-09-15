mod server;
use search_engine::storage;
use std::path::Path;

#[tokio::main]
async fn main() {
    let input = Path::new("./some");
    let output = Path::new("./index_data");

    // index any new files first
    if let Err(e) = storage::ingest::build_index(input, output).await {
        eprintln!("index error: {e}");
        return;
    }

    // start server (background merge + future query handling)
    let server = server::Server::new(output);
    if let Err(e) = server.start().await {
        eprintln!("server error: {e}");
    }
}
