use super::{errors::FsError, fs::PTR_SIZE};
use faster_hex::hex_string;
use std::{
    convert::{TryFrom, TryInto},
    path::PathBuf,
};

pub const BLOCKID_SIZE: usize = 16;

pub type BlockID = [u8; BLOCKID_SIZE]; // Size of an md5 hash

// TODO: this can be optimized by making path a `[u8;BLOCKID_SIZE]` and keeping track of a len u8
#[derive(Debug)]
pub struct Block {
    size: usize,
    path: Vec<u8>,
    #[cfg(feature = "refcount")]
    rc: usize,
}

impl From<&Block> for Vec<u8> {
    fn from(b: &Block) -> Self {
        // NOTE: we encode the lenght of the vector as a single byte, since it can only be 16 bytes
        // long.
        #[cfg(not(feature = "refcount"))]
        let mut out = Vec::with_capacity(PTR_SIZE + b.path.len() + 1);
        #[cfg(feature = "refcount")]
        let mut out = Vec::with_capacity(2 * PTR_SIZE + b.path.len() + 1);

        out.extend_from_slice(&b.size.to_le_bytes());
        out.extend_from_slice(&(b.path.len() as u8).to_le_bytes());
        out.extend_from_slice(&b.path);
        #[cfg(feature = "refcount")]
        out.extend_from_slice(&b.rc.to_le_bytes());
        out
    }
}

impl TryFrom<&[u8]> for Block {
    type Error = FsError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < PTR_SIZE + 1 {
            return Err(FsError::MalformedObject);
        }
        let size = usize::from_le_bytes(value[..PTR_SIZE].try_into().unwrap());

        let vec_size =
            u8::from_le_bytes(value[PTR_SIZE..PTR_SIZE + 1].try_into().unwrap()) as usize;
        if value.len() < PTR_SIZE + 1 + vec_size {
            return Err(FsError::MalformedObject);
        }
        let path = value[PTR_SIZE + 1..PTR_SIZE + 1 + vec_size].to_vec();

        #[cfg(not(feature = "refcount"))]
        if value.len() != PTR_SIZE + 1 + vec_size {
            return Err(FsError::MalformedObject);
        }

        #[cfg(feature = "refcount")]
        if value.len() != PTR_SIZE * 2 + 1 + vec_size {
            return Err(FsError::MalformedObject);
        }

        Ok(Block {
            size,
            path,
            #[cfg(feature = "refcount")]
            rc: usize::from_le_bytes(value[PTR_SIZE + 1 + vec_size..].try_into().unwrap()),
        })
    }
}

impl Block {
    pub fn new(size: usize, path: Vec<u8>) -> Self {
        Self {
            size,
            path,
            #[cfg(feature = "refcount")]
            rc: 1,
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn path(&self) -> &[u8] {
        &self.path
    }

    pub fn disk_path(&self, mut root: PathBuf) -> PathBuf {
        // path has at least len 1
        let dirs = &self.path[..self.path.len() - 1];
        for byte in dirs {
            root.push(hex_string(&[*byte]));
        }
        root.push(format!(
            "_{}",
            hex_string(&[self.path[self.path.len() - 1]])
        ));
        root
    }

    #[cfg(feature = "refcount")]
    pub fn rc(&self) -> usize {
        self.rc
    }

    #[cfg(feature = "refcount")]
    pub fn increment_refcount(&mut self) {
        self.rc += 1
    }

    #[cfg(feature = "refcount")]
    pub fn decrement_refcount(&mut self) {
        self.rc -= 1
    }
}
