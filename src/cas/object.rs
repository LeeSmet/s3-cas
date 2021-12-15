use super::{
    block::{BlockID, BLOCKID_SIZE},
    errors::FsError,
    fs::PTR_SIZE,
};
use chrono::Utc;
use faster_hex::hex_string;
use std::convert::{TryFrom, TryInto};

#[derive(Debug)]
pub struct Object {
    size: u64,
    ctime: i64,
    e_tag: BlockID,
    // The amount of parts uploaded for this object. In case of a simple put_object, this will be
    // 0. In case of a multipart upload, this wil equal the amount of individual parts. This is
    // required so we can properly construct the formatted hash later.
    parts: usize,
    blocks: Vec<BlockID>,
}

impl Object {
    pub fn new(size: u64, e_tag: BlockID, parts: usize, blocks: Vec<BlockID>) -> Self {
        Self {
            size,
            ctime: Utc::now().timestamp(),
            e_tag,
            parts,
            blocks,
        }
    }

    pub fn format_e_tag(&self) -> String {
        if self.parts == 0 {
            format!("\"{}\"", hex_string(&self.e_tag))
        } else {
            format!("\"{}-{}\"", hex_string(&self.e_tag), self.parts)
        }
    }

    pub fn touch(&mut self) {
        self.ctime = Utc::now().timestamp();
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn ctime(&self) -> i64 {
        self.ctime
    }

    pub fn parts(&self) -> usize {
        self.parts
    }

    pub fn blocks(&self) -> &[BlockID] {
        &self.blocks
    }
}

impl From<&Object> for Vec<u8> {
    fn from(o: &Object) -> Self {
        let mut data =
            Vec::with_capacity(16 + BLOCKID_SIZE + PTR_SIZE * 2 + o.blocks.len() * BLOCKID_SIZE);

        data.extend_from_slice(&o.size.to_le_bytes());
        data.extend_from_slice(&o.ctime.to_le_bytes());
        data.extend_from_slice(&o.e_tag);
        data.extend_from_slice(&o.parts.to_le_bytes());
        data.extend_from_slice(&o.blocks.len().to_le_bytes());
        for block in &o.blocks {
            data.extend_from_slice(block);
        }

        data
    }
}

impl TryFrom<&[u8]> for Object {
    type Error = FsError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < 16 + BLOCKID_SIZE + 2 * PTR_SIZE {
            return Err(FsError::MalformedObject);
        }

        let block_len = usize::from_le_bytes(
            value[16 + BLOCKID_SIZE + PTR_SIZE..16 + BLOCKID_SIZE + 2 * PTR_SIZE]
                .try_into()
                .unwrap(),
        );

        if value.len() != 16 + 2 * PTR_SIZE + BLOCKID_SIZE + block_len * BLOCKID_SIZE {
            return Err(FsError::MalformedObject);
        }

        let mut blocks = Vec::with_capacity(block_len);

        for chunk in value[16 + 2 * PTR_SIZE + BLOCKID_SIZE..].chunks_exact(BLOCKID_SIZE) {
            blocks.push(chunk.try_into().unwrap());
        }

        Ok(Object {
            size: u64::from_le_bytes(value[0..8].try_into().unwrap()),
            ctime: i64::from_le_bytes(value[8..16].try_into().unwrap()),
            e_tag: value[16..16 + BLOCKID_SIZE].try_into().unwrap(),
            parts: usize::from_le_bytes(
                value[16 + BLOCKID_SIZE..16 + BLOCKID_SIZE + PTR_SIZE]
                    .try_into()
                    .unwrap(),
            ),
            blocks,
        })
    }
}
