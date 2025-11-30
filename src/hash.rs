use std::ops::BitXor;

const SEED: u64 = 0x517cc1b727220a95;

pub struct MyHasher {
    state: u64,
}

impl std::hash::Hasher for MyHasher {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.state
    }

    #[inline(always)]
    fn write(&mut self, mut bytes: &[u8]) {
        while bytes.len() >= 8 {
            let n = u64::from_ne_bytes(bytes[..8].try_into().unwrap());
            self.state = self.state.bitxor(n).wrapping_mul(SEED);
            bytes = &bytes[8..];
        }

        if bytes.len() >= 4 {
            let n = u32::from_ne_bytes(bytes[..4].try_into().unwrap());
            self.state = self.state.bitxor(n as u64).wrapping_mul(SEED);
            bytes = &bytes[4..];
        }

        for byte in bytes {
            self.state = self.state.bitxor(*byte as u64).wrapping_mul(SEED);
        }
    }
}

#[derive(Default)]
pub struct MyHasherBuilder;

impl std::hash::BuildHasher for MyHasherBuilder {
    type Hasher = MyHasher;

    #[inline(always)]
    fn build_hasher(&self) -> MyHasher {
        MyHasher { state: 0 }
    }
}

pub type MyHashMap<K, V> = std::collections::HashMap<K, V, MyHasherBuilder>;
