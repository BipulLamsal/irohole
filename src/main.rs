use iroh::{protocol::Router, Endpoint};
use iroh_blobs::{store::mem::MemStore, BlobsProtocol};
use iroh_blobs::ticket::BlobTicket;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // create an iroh endpoint that includes the standard discovery mechanisms
    let endpoint = Endpoint::builder().discovery_local_network().bind().await?;

    // create a protocol handler using an in-memory blob store.
    let store = MemStore::new();
    let blobs = BlobsProtocol::new(&store, endpoint.clone(), None);

    // find the file
    let mut path = PathBuf::new();
    path.push("test.txt");
    let absolute = std::path::absolute(&path)?;

    // get the tag of the file
    println!("Hashing the file");
    let tag = store.blobs().add_path(absolute).await?;
    // generate the ticket
    let node_id = endpoint.node_id();
    let ticket = BlobTicket::new(node_id.into(), tag.hash, tag.format);

    println!("Here is NodeId: {}, ticket :{}, tag: {:?}",node_id,ticket, tag.hash);
    // build the router
    let router = Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs.clone())
        .spawn();

    println!("We are now serving {}", ticket);

    // wait for control-c
    let _ = tokio::signal::ctrl_c().await;

    // clean shutdown of router and store
    router.shutdown().await?;
    Ok(())
}
