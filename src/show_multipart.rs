use serde::{Deserialize, Serialize};
use std::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 2 {
        eprint!("Usage: show_multipart $path_to_db");
    }
    let db = sled::open(&args[1]).unwrap();

    let mpt = db.open_tree("_MULTIPART_PARTS").unwrap();
    for res in mpt.iter() {
        let (key, value) = res.unwrap();
        let v = serde_json::from_slice::<MultiPart>(&value).unwrap();

        println!("{}:\n{:?}\n\n", String::from_utf8(key.to_vec()).unwrap(), v);
    }

    Ok(())
}

type BlockID = [u8; BLOCKID_SIZE]; // Size of an md5 hash
const BLOCKID_SIZE: usize = 16;

#[derive(Debug, Serialize, Deserialize)]
struct MultiPart {
    size: usize,
    part_number: i64,
    bucket: String,
    key: String,
    upload_id: String,
    hash: BlockID,
    blocks: Vec<BlockID>,
}
