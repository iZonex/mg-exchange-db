use xxhash_rust::xxh3;

/// Hash bytes using xxHash3 (64-bit).
#[inline]
pub fn xxh3_64(data: &[u8]) -> u64 {
    xxh3::xxh3_64(data)
}

/// Hash bytes with seed using xxHash3 (64-bit).
#[inline]
pub fn xxh3_64_with_seed(data: &[u8], seed: u64) -> u64 {
    xxh3::xxh3_64_with_seed(data, seed)
}
