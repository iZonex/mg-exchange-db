//! Encryption at rest for ExchangeDB column data files.
//!
//! Provides AES-256-GCM-equivalent authenticated encryption using ChaCha20-Poly1305.
//! ChaCha20 is a stream cipher that is equally secure to AES-256 and simpler to
//! implement correctly in software (no need for hardware AES-NI).
//!
//! Legacy XOR cipher is retained for backward compatibility when reading old files.

use std::fs;
use std::path::{Path, PathBuf};

use exchange_common::error::{ExchangeDbError, Result};
use rand::Rng;
use sha2::{Digest, Sha256};

/// Magic bytes identifying an encrypted file.
const MAGIC: &[u8; 4] = b"XENC";

/// Legacy header size: 4 (magic) + 1 (algorithm) + 16 (IV) = 21 bytes.
const LEGACY_HEADER_SIZE: usize = 21;

/// New header size: 4 (magic) + 1 (algorithm) + 12 (nonce) + 16 (Poly1305 tag) = 33 bytes.
const HEADER_SIZE: usize = 33;

/// Encryption algorithm selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionAlgorithm {
    /// AES-256-CBC placeholder (legacy XOR for backward compat reading).
    Aes256Cbc,
    /// AES-256-GCM equivalent — implemented as ChaCha20-Poly1305.
    Aes256Gcm,
    /// ChaCha20-Poly1305 authenticated encryption (new default).
    ChaCha20Poly1305,
}

impl EncryptionAlgorithm {
    fn to_byte(self) -> u8 {
        match self {
            EncryptionAlgorithm::Aes256Cbc => 1,
            EncryptionAlgorithm::Aes256Gcm => 2,
            EncryptionAlgorithm::ChaCha20Poly1305 => 3,
        }
    }

    fn from_byte(b: u8) -> Result<Self> {
        match b {
            1 => Ok(EncryptionAlgorithm::Aes256Cbc),
            2 => Ok(EncryptionAlgorithm::Aes256Gcm),
            3 => Ok(EncryptionAlgorithm::ChaCha20Poly1305),
            _ => Err(ExchangeDbError::Corruption(format!(
                "unknown encryption algorithm byte: {b}"
            ))),
        }
    }

    /// Whether this algorithm uses the legacy XOR cipher.
    fn is_legacy(self) -> bool {
        matches!(self, EncryptionAlgorithm::Aes256Cbc)
    }
}

/// Configuration for encryption at rest.
#[derive(Debug, Clone)]
pub struct EncryptionConfig {
    /// Whether encryption is enabled.
    pub enabled: bool,
    /// Algorithm to use for *new* encryptions.
    pub algorithm: EncryptionAlgorithm,
    /// Encryption key (32 bytes for AES-256 / ChaCha20).
    pub key: Vec<u8>,
}

impl EncryptionConfig {
    /// Create a new encryption config with a 32-byte key.
    pub fn new(algorithm: EncryptionAlgorithm, key: Vec<u8>) -> Result<Self> {
        if key.len() != 32 {
            return Err(ExchangeDbError::Corruption(format!(
                "encryption key must be 32 bytes, got {}",
                key.len()
            )));
        }
        Ok(Self {
            enabled: true,
            algorithm,
            key,
        })
    }

    /// Create a disabled encryption config.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            algorithm: EncryptionAlgorithm::ChaCha20Poly1305,
            key: Vec::new(),
        }
    }
}

// ===========================================================================
// ChaCha20 stream cipher
// ===========================================================================

/// ChaCha20 quarter-round operation on state words.
#[inline(always)]
fn quarter_round(state: &mut [u32; 16], a: usize, b: usize, c: usize, d: usize) {
    state[a] = state[a].wrapping_add(state[b]);
    state[d] ^= state[a];
    state[d] = state[d].rotate_left(16);

    state[c] = state[c].wrapping_add(state[d]);
    state[b] ^= state[c];
    state[b] = state[b].rotate_left(12);

    state[a] = state[a].wrapping_add(state[b]);
    state[d] ^= state[a];
    state[d] = state[d].rotate_left(8);

    state[c] = state[c].wrapping_add(state[d]);
    state[b] ^= state[c];
    state[b] = state[b].rotate_left(7);
}

/// Generate a 64-byte ChaCha20 keystream block.
fn chacha20_block(key: &[u8; 32], nonce: &[u8; 12], counter: u32) -> [u8; 64] {
    // "expand 32-byte k"
    let mut state: [u32; 16] = [
        0x61707865,
        0x3320646e,
        0x79622d32,
        0x6b206574,
        u32::from_le_bytes(key[0..4].try_into().unwrap()),
        u32::from_le_bytes(key[4..8].try_into().unwrap()),
        u32::from_le_bytes(key[8..12].try_into().unwrap()),
        u32::from_le_bytes(key[12..16].try_into().unwrap()),
        u32::from_le_bytes(key[16..20].try_into().unwrap()),
        u32::from_le_bytes(key[20..24].try_into().unwrap()),
        u32::from_le_bytes(key[24..28].try_into().unwrap()),
        u32::from_le_bytes(key[28..32].try_into().unwrap()),
        counter,
        u32::from_le_bytes(nonce[0..4].try_into().unwrap()),
        u32::from_le_bytes(nonce[4..8].try_into().unwrap()),
        u32::from_le_bytes(nonce[8..12].try_into().unwrap()),
    ];

    let initial = state;

    // 20 rounds (10 double-rounds)
    for _ in 0..10 {
        // Column rounds
        quarter_round(&mut state, 0, 4, 8, 12);
        quarter_round(&mut state, 1, 5, 9, 13);
        quarter_round(&mut state, 2, 6, 10, 14);
        quarter_round(&mut state, 3, 7, 11, 15);
        // Diagonal rounds
        quarter_round(&mut state, 0, 5, 10, 15);
        quarter_round(&mut state, 1, 6, 11, 12);
        quarter_round(&mut state, 2, 7, 8, 13);
        quarter_round(&mut state, 3, 4, 9, 14);
    }

    // Add initial state
    for i in 0..16 {
        state[i] = state[i].wrapping_add(initial[i]);
    }

    // Serialize to bytes
    let mut output = [0u8; 64];
    for (i, &word) in state.iter().enumerate() {
        output[i * 4..i * 4 + 4].copy_from_slice(&word.to_le_bytes());
    }
    output
}

/// Encrypt/decrypt data using ChaCha20 stream cipher (symmetric).
fn chacha20_encrypt(data: &[u8], key: &[u8; 32], nonce: &[u8; 12]) -> Vec<u8> {
    chacha20_encrypt_with_counter(data, key, nonce, 1)
}

/// Encrypt/decrypt data using ChaCha20 starting from the given counter.
fn chacha20_encrypt_with_counter(
    data: &[u8],
    key: &[u8; 32],
    nonce: &[u8; 12],
    start_counter: u32,
) -> Vec<u8> {
    let mut output = Vec::with_capacity(data.len());
    let mut counter = start_counter;

    for chunk in data.chunks(64) {
        let block = chacha20_block(key, nonce, counter);
        for (i, &byte) in chunk.iter().enumerate() {
            output.push(byte ^ block[i]);
        }
        counter += 1;
    }

    output
}

// ===========================================================================
// Poly1305 MAC
// ===========================================================================

/// Poly1305 one-time authenticator.
/// Computes a 16-byte tag over the message using a one-time key.
///
/// Implements the algorithm from RFC 8439 using 5 x 26-bit limbs with u64
/// intermediates, following the donna reference implementation approach.
fn poly1305_mac(msg: &[u8], key: &[u8; 32]) -> [u8; 16] {
    // Clamp r (key[0..16])
    let mut r_bytes = [0u8; 16];
    r_bytes.copy_from_slice(&key[0..16]);
    r_bytes[3] &= 15;
    r_bytes[7] &= 15;
    r_bytes[11] &= 15;
    r_bytes[15] &= 15;
    r_bytes[4] &= 252;
    r_bytes[8] &= 252;
    r_bytes[12] &= 252;

    let s = &key[16..32];

    // Load r as a 128-bit little-endian number, then split into 5 x 26-bit limbs
    let r_full = u128::from_le_bytes(r_bytes);
    let r0 = (r_full & 0x03ff_ffff) as u64;
    let r1 = ((r_full >> 26) & 0x03ff_ffff) as u64;
    let r2 = ((r_full >> 52) & 0x03ff_ffff) as u64;
    let r3 = ((r_full >> 78) & 0x03ff_ffff) as u64;
    let r4 = ((r_full >> 104) & 0x03ff_ffff) as u64;

    // Precompute r*5 values for reduction
    let s1 = r1 * 5;
    let s2 = r2 * 5;
    let s3 = r3 * 5;
    let s4 = r4 * 5;

    let mut h0: u64 = 0;
    let mut h1: u64 = 0;
    let mut h2: u64 = 0;
    let mut h3: u64 = 0;
    let mut h4: u64 = 0;

    // Process 16-byte blocks
    let mut i = 0;
    while i < msg.len() {
        let end = std::cmp::min(i + 16, msg.len());
        let block_len = end - i;

        // Read block as a little-endian 17-byte number (with high bit set)
        let mut block = [0u8; 17];
        block[..block_len].copy_from_slice(&msg[i..end]);
        block[block_len] = 1; // high bit

        // Convert to 130-bit number in 5 x 26-bit limbs
        let n_full = u128::from_le_bytes(block[..16].try_into().unwrap());
        let hibit = block[16] as u64;

        let t0 = (n_full & 0x03ff_ffff) as u64;
        let t1 = ((n_full >> 26) & 0x03ff_ffff) as u64;
        let t2 = ((n_full >> 52) & 0x03ff_ffff) as u64;
        let t3 = ((n_full >> 78) & 0x03ff_ffff) as u64;
        let t4 = ((n_full >> 104) as u64) | (hibit << 24);

        // h += n
        h0 += t0;
        h1 += t1;
        h2 += t2;
        h3 += t3;
        h4 += t4;

        // h *= r (mod 2^130 - 5) using partial products
        // Each d_i = sum of h_j * r_k where j+k = i (mod 5, with *5 reduction)
        // Using u128 for the products to avoid overflow
        let d0 = (h0 as u128) * (r0 as u128)
            + (h1 as u128) * (s4 as u128)
            + (h2 as u128) * (s3 as u128)
            + (h3 as u128) * (s2 as u128)
            + (h4 as u128) * (s1 as u128);

        let d1 = (h0 as u128) * (r1 as u128)
            + (h1 as u128) * (r0 as u128)
            + (h2 as u128) * (s4 as u128)
            + (h3 as u128) * (s3 as u128)
            + (h4 as u128) * (s2 as u128);

        let d2 = (h0 as u128) * (r2 as u128)
            + (h1 as u128) * (r1 as u128)
            + (h2 as u128) * (r0 as u128)
            + (h3 as u128) * (s4 as u128)
            + (h4 as u128) * (s3 as u128);

        let d3 = (h0 as u128) * (r3 as u128)
            + (h1 as u128) * (r2 as u128)
            + (h2 as u128) * (r1 as u128)
            + (h3 as u128) * (r0 as u128)
            + (h4 as u128) * (s4 as u128);

        let d4 = (h0 as u128) * (r4 as u128)
            + (h1 as u128) * (r3 as u128)
            + (h2 as u128) * (r2 as u128)
            + (h3 as u128) * (r1 as u128)
            + (h4 as u128) * (r0 as u128);

        // Partial reduction: propagate carries
        let mut c: u64;
        h0 = (d0 & 0x03ff_ffff) as u64;
        c = (d0 >> 26) as u64;

        let d1 = d1 + c as u128;
        h1 = (d1 & 0x03ff_ffff) as u64;
        c = (d1 >> 26) as u64;

        let d2 = d2 + c as u128;
        h2 = (d2 & 0x03ff_ffff) as u64;
        c = (d2 >> 26) as u64;

        let d3 = d3 + c as u128;
        h3 = (d3 & 0x03ff_ffff) as u64;
        c = (d3 >> 26) as u64;

        let d4 = d4 + c as u128;
        h4 = (d4 & 0x03ff_ffff) as u64;
        c = (d4 >> 26) as u64;

        // 2^130 ≡ 5 (mod P), so carry out of h4 wraps around as *5
        h0 += c * 5;
        c = h0 >> 26;
        h0 &= 0x03ff_ffff;
        h1 += c;

        i = end;
    }

    // Final reduction: fully reduce h mod P = 2^130 - 5
    let mut c: u64;
    c = h1 >> 26;
    h1 &= 0x03ff_ffff;
    h2 += c;
    c = h2 >> 26;
    h2 &= 0x03ff_ffff;
    h3 += c;
    c = h3 >> 26;
    h3 &= 0x03ff_ffff;
    h4 += c;
    c = h4 >> 26;
    h4 &= 0x03ff_ffff;
    h0 += c * 5;
    c = h0 >> 26;
    h0 &= 0x03ff_ffff;
    h1 += c;

    // Compute h + -(2^130 - 5) = h - P. If no borrow, h >= P so use g = h - P.
    let mut g0 = h0.wrapping_add(5);
    c = g0 >> 26;
    g0 &= 0x03ff_ffff;
    let mut g1 = h1.wrapping_add(c);
    c = g1 >> 26;
    g1 &= 0x03ff_ffff;
    let mut g2 = h2.wrapping_add(c);
    c = g2 >> 26;
    g2 &= 0x03ff_ffff;
    let mut g3 = h3.wrapping_add(c);
    c = g3 >> 26;
    g3 &= 0x03ff_ffff;
    let g4 = h4.wrapping_add(c).wrapping_sub(1 << 26);

    // Select h or g: if g4's top bit is set (borrow), keep h; otherwise use g
    // g4 bit 63 is set when the subtraction borrowed (h < P), meaning we should keep h
    let mask = (g4 >> 63).wrapping_neg(); // all 1s if borrow (keep h), all 0s if no borrow (use g)
    h0 = (h0 & mask) | (g0 & !mask);
    h1 = (h1 & mask) | (g1 & !mask);
    h2 = (h2 & mask) | (g2 & !mask);
    h3 = (h3 & mask) | (g3 & !mask);
    h4 = (h4 & mask) | (g4 & !mask);

    // Assemble h as a 128-bit number from 26-bit limbs
    let h_128: u128 = (h0 as u128)
        | ((h1 as u128) << 26)
        | ((h2 as u128) << 52)
        | ((h3 as u128) << 78)
        | ((h4 as u128) << 104);

    // Add s (key[16..32]) and output mod 2^128
    let s_128 = u128::from_le_bytes(s.try_into().unwrap());
    let tag = h_128.wrapping_add(s_128);

    tag.to_le_bytes()
}

/// Construct the Poly1305 AEAD message: AAD || ciphertext || padding || len(AAD) || len(CT)
fn poly1305_aead_construct(aad: &[u8], ciphertext: &[u8]) -> Vec<u8> {
    let mut msg = Vec::with_capacity(aad.len() + 16 + ciphertext.len() + 16 + 16);

    // AAD padded to 16-byte boundary
    msg.extend_from_slice(aad);
    let aad_pad = (16 - (aad.len() % 16)) % 16;
    msg.extend(std::iter::repeat_n(0u8, aad_pad));

    // Ciphertext padded to 16-byte boundary
    msg.extend_from_slice(ciphertext);
    let ct_pad = (16 - (ciphertext.len() % 16)) % 16;
    msg.extend(std::iter::repeat_n(0u8, ct_pad));

    // Lengths as 64-bit little-endian
    msg.extend_from_slice(&(aad.len() as u64).to_le_bytes());
    msg.extend_from_slice(&(ciphertext.len() as u64).to_le_bytes());

    msg
}

// ===========================================================================
// ChaCha20-Poly1305 AEAD
// ===========================================================================

/// Encrypt using ChaCha20-Poly1305 AEAD.
/// Returns (ciphertext, 16-byte tag).
fn chacha20_poly1305_encrypt(
    plaintext: &[u8],
    key: &[u8; 32],
    nonce: &[u8; 12],
    aad: &[u8],
) -> (Vec<u8>, [u8; 16]) {
    // Generate Poly1305 one-time key from ChaCha20 block 0
    let otk_block = chacha20_block(key, nonce, 0);
    let mut otk = [0u8; 32];
    otk.copy_from_slice(&otk_block[0..32]);

    // Encrypt with ChaCha20 starting at counter 1
    let ciphertext = chacha20_encrypt(plaintext, key, nonce);

    // Compute tag
    let mac_data = poly1305_aead_construct(aad, &ciphertext);
    let tag = poly1305_mac(&mac_data, &otk);

    (ciphertext, tag)
}

/// Decrypt using ChaCha20-Poly1305 AEAD.
/// Returns plaintext or error if authentication fails.
fn chacha20_poly1305_decrypt(
    ciphertext: &[u8],
    key: &[u8; 32],
    nonce: &[u8; 12],
    aad: &[u8],
    expected_tag: &[u8; 16],
) -> Result<Vec<u8>> {
    // Generate Poly1305 one-time key from ChaCha20 block 0
    let otk_block = chacha20_block(key, nonce, 0);
    let mut otk = [0u8; 32];
    otk.copy_from_slice(&otk_block[0..32]);

    // Verify tag
    let mac_data = poly1305_aead_construct(aad, ciphertext);
    let computed_tag = poly1305_mac(&mac_data, &otk);

    // Constant-time comparison
    let mut diff = 0u8;
    for i in 0..16 {
        diff |= computed_tag[i] ^ expected_tag[i];
    }
    if diff != 0 {
        return Err(ExchangeDbError::Corruption(
            "authentication tag mismatch: data may be corrupted or wrong key".into(),
        ));
    }

    // Decrypt with ChaCha20 starting at counter 1
    Ok(chacha20_encrypt(ciphertext, key, nonce))
}

// ===========================================================================
// Key derivation
// ===========================================================================

/// Derive an encryption key and nonce from the user key using SHA-256 based KDF.
/// This provides domain separation so the same user key with different contexts
/// produces different derived keys.
fn derive_key_nonce(user_key: &[u8; 32], context: &[u8]) -> ([u8; 32], [u8; 12]) {
    // Derive key: SHA-256(user_key || "chacha20-key" || context)
    let mut hasher = Sha256::new();
    hasher.update(user_key);
    hasher.update(b"chacha20-key");
    hasher.update(context);
    let derived_key: [u8; 32] = hasher.finalize().into();

    // Derive nonce: SHA-256(user_key || "chacha20-nonce" || context)[..12]
    let mut hasher = Sha256::new();
    hasher.update(user_key);
    hasher.update(b"chacha20-nonce");
    hasher.update(context);
    let nonce_hash: [u8; 32] = hasher.finalize().into();
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&nonce_hash[..12]);

    (derived_key, nonce)
}

// ===========================================================================
// Legacy XOR cipher (for backward compatibility)
// ===========================================================================

/// Generate a simple IV from the key (legacy, deterministic).
fn generate_iv(key: &[u8]) -> [u8; 16] {
    let mut iv = [0u8; 16];
    for (i, b) in key.iter().take(16).enumerate() {
        iv[i] = b.wrapping_mul(0x9E).wrapping_add(0x37);
    }
    iv
}

/// XOR-encrypt/decrypt `data` using `key` and `iv` (legacy).
fn xor_cipher(data: &[u8], key: &[u8], iv: &[u8]) -> Vec<u8> {
    let combined_key_len = key.len() + iv.len();
    data.iter()
        .enumerate()
        .map(|(i, &b)| {
            let ki = i % combined_key_len;
            let key_byte = if ki < key.len() {
                key[ki]
            } else {
                iv[ki - key.len()]
            };
            b ^ key_byte
        })
        .collect()
}

// ===========================================================================
// Header encoding / decoding
// ===========================================================================

/// Build the encryption header for new (authenticated) format.
/// Layout: MAGIC(4) + algo(1) + nonce(12) + tag(16) = 33 bytes.
fn build_header(algorithm: EncryptionAlgorithm, nonce: &[u8; 12], tag: &[u8; 16]) -> Vec<u8> {
    let mut header = Vec::with_capacity(HEADER_SIZE);
    header.extend_from_slice(MAGIC);
    header.push(algorithm.to_byte());
    header.extend_from_slice(nonce);
    header.extend_from_slice(tag);
    header
}

/// Build legacy header: MAGIC(4) + algo(1) + IV(16) = 21 bytes.
fn build_legacy_header(algorithm: EncryptionAlgorithm, iv: &[u8; 16]) -> Vec<u8> {
    let mut header = Vec::with_capacity(LEGACY_HEADER_SIZE);
    header.extend_from_slice(MAGIC);
    header.push(algorithm.to_byte());
    header.extend_from_slice(iv);
    header
}

/// Parse the encryption header. Detects legacy vs new format based on algorithm byte.
/// Returns (algorithm, header_size, nonce_or_iv, optional_tag).
fn parse_header(data: &[u8]) -> Result<ParsedHeader> {
    if data.len() < 5 {
        return Err(ExchangeDbError::Corruption(
            "encrypted data too short for header".into(),
        ));
    }
    if &data[0..4] != MAGIC {
        return Err(ExchangeDbError::Corruption(
            "missing encryption magic bytes".into(),
        ));
    }
    let algorithm = EncryptionAlgorithm::from_byte(data[4])?;

    match algorithm {
        EncryptionAlgorithm::Aes256Cbc => {
            // Legacy format with 16-byte IV
            if data.len() < LEGACY_HEADER_SIZE {
                return Err(ExchangeDbError::Corruption(
                    "encrypted data too short for legacy header".into(),
                ));
            }
            let mut iv = [0u8; 16];
            iv.copy_from_slice(&data[5..21]);
            Ok(ParsedHeader::Legacy {
                algorithm,
                iv,
                data_offset: LEGACY_HEADER_SIZE,
            })
        }
        EncryptionAlgorithm::Aes256Gcm | EncryptionAlgorithm::ChaCha20Poly1305 => {
            // New format with 12-byte nonce + 16-byte tag
            if data.len() < HEADER_SIZE {
                return Err(ExchangeDbError::Corruption(
                    "encrypted data too short for header".into(),
                ));
            }
            let mut nonce = [0u8; 12];
            nonce.copy_from_slice(&data[5..17]);
            let mut tag = [0u8; 16];
            tag.copy_from_slice(&data[17..33]);
            Ok(ParsedHeader::Authenticated {
                algorithm,
                nonce,
                tag,
                data_offset: HEADER_SIZE,
            })
        }
    }
}

enum ParsedHeader {
    Legacy {
        #[allow(dead_code)]
        algorithm: EncryptionAlgorithm,
        iv: [u8; 16],
        data_offset: usize,
    },
    Authenticated {
        algorithm: EncryptionAlgorithm,
        nonce: [u8; 12],
        tag: [u8; 16],
        data_offset: usize,
    },
}

// ===========================================================================
// Public API
// ===========================================================================

/// Encrypt a data buffer, returning header + ciphertext.
///
/// Uses ChaCha20-Poly1305 authenticated encryption with a random nonce.
/// The algorithm byte in the config determines what is written to the header.
pub fn encrypt_buffer(data: &[u8], config: &EncryptionConfig) -> Result<Vec<u8>> {
    if !config.enabled {
        return Ok(data.to_vec());
    }

    let key: &[u8; 32] = config.key.as_slice().try_into().map_err(|_| {
        ExchangeDbError::Corruption("encryption key must be exactly 32 bytes".into())
    })?;

    if config.algorithm.is_legacy() {
        // Legacy XOR path for Aes256Cbc
        let iv = generate_iv(&config.key);
        let encrypted = xor_cipher(data, &config.key, &iv);
        let mut result = build_legacy_header(config.algorithm, &iv);
        result.extend_from_slice(&encrypted);
        return Ok(result);
    }

    // Generate random 12-byte nonce
    let mut nonce = [0u8; 12];
    rand::thread_rng().fill(&mut nonce);

    // Derive per-message key from user key + nonce for extra safety
    let (derived_key, _) = derive_key_nonce(key, &nonce);

    // AAD is the algorithm byte (binds the ciphertext to the algorithm choice)
    let aad = [config.algorithm.to_byte()];

    let (ciphertext, tag) = chacha20_poly1305_encrypt(data, &derived_key, &nonce, &aad);

    let mut result = build_header(config.algorithm, &nonce, &tag);
    result.extend_from_slice(&ciphertext);
    Ok(result)
}

/// Decrypt a data buffer (header + ciphertext), returning plaintext.
///
/// Automatically detects legacy (XOR) vs authenticated (ChaCha20-Poly1305) format
/// based on the algorithm byte in the header.
pub fn decrypt_buffer(encrypted: &[u8], config: &EncryptionConfig) -> Result<Vec<u8>> {
    if !config.enabled {
        return Ok(encrypted.to_vec());
    }

    let key: &[u8; 32] = config.key.as_slice().try_into().map_err(|_| {
        ExchangeDbError::Corruption("encryption key must be exactly 32 bytes".into())
    })?;

    match parse_header(encrypted)? {
        ParsedHeader::Legacy { iv, data_offset, .. } => {
            // Legacy XOR decryption
            let ciphertext = &encrypted[data_offset..];
            Ok(xor_cipher(ciphertext, &config.key, &iv))
        }
        ParsedHeader::Authenticated {
            algorithm,
            nonce,
            tag,
            data_offset,
        } => {
            let ciphertext = &encrypted[data_offset..];

            // Derive per-message key from user key + nonce
            let (derived_key, _) = derive_key_nonce(key, &nonce);

            // AAD is the algorithm byte
            let aad = [algorithm.to_byte()];

            chacha20_poly1305_decrypt(ciphertext, &derived_key, &nonce, &aad, &tag)
        }
    }
}

/// Encrypt a column data file in-place. The encrypted file is written to
/// `<path>.enc`.
pub fn encrypt_file(path: &Path, config: &EncryptionConfig) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }
    let data = fs::read(path)?;
    let encrypted = encrypt_buffer(&data, config)?;
    let enc_path = encrypted_path(path);
    fs::write(&enc_path, &encrypted)?;
    Ok(())
}

/// Decrypt a column data file in-place. Reads from `<path>.enc` and writes
/// decrypted content to `<path>`.
pub fn decrypt_file(path: &Path, config: &EncryptionConfig) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }
    // If the given path already ends in ".enc", read from it directly and
    // write the decrypted content to the path without the ".enc" suffix.
    let path_str = path.to_string_lossy();
    let (enc_path, out_path) = if path_str.ends_with(".enc") {
        let original = PathBuf::from(path_str.trim_end_matches(".enc"));
        (path.to_path_buf(), original)
    } else {
        (encrypted_path(path), path.to_path_buf())
    };
    let encrypted = fs::read(&enc_path)?;
    let decrypted = decrypt_buffer(&encrypted, config)?;
    fs::write(&out_path, &decrypted)?;
    Ok(())
}

/// Return the `.enc` variant of a path.
fn encrypted_path(path: &Path) -> PathBuf {
    let mut p = path.as_os_str().to_owned();
    p.push(".enc");
    PathBuf::from(p)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_config() -> EncryptionConfig {
        EncryptionConfig::new(EncryptionAlgorithm::ChaCha20Poly1305, vec![0xAB; 32]).unwrap()
    }

    fn gcm_config() -> EncryptionConfig {
        EncryptionConfig::new(EncryptionAlgorithm::Aes256Gcm, vec![0xAB; 32]).unwrap()
    }

    fn legacy_config() -> EncryptionConfig {
        EncryptionConfig::new(EncryptionAlgorithm::Aes256Cbc, vec![0xAB; 32]).unwrap()
    }

    // -- ChaCha20 unit tests ------------------------------------------------

    #[test]
    fn chacha20_block_known_vector() {
        // RFC 8439 Section 2.3.2 test vector
        let key = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ];
        let nonce = [
            0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x4a, 0x00, 0x00, 0x00, 0x00,
        ];
        let counter = 1;
        let block = chacha20_block(&key, &nonce, counter);

        // First 4 bytes of the expected output from RFC 8439 test vector
        assert_eq!(block[0], 0x10);
        assert_eq!(block[1], 0xf1);
        assert_eq!(block[2], 0xe7);
        assert_eq!(block[3], 0xe4);
    }

    #[test]
    fn chacha20_encrypt_decrypt_roundtrip() {
        let key = [0x42u8; 32];
        let nonce = [0x01u8; 12];
        let plaintext = b"Hello, ChaCha20!";

        let ciphertext = chacha20_encrypt(plaintext, &key, &nonce);
        assert_ne!(&ciphertext[..], plaintext);

        // ChaCha20 is symmetric: encrypt again to decrypt
        let decrypted = chacha20_encrypt(&ciphertext, &key, &nonce);
        assert_eq!(&decrypted[..], plaintext);
    }

    // -- Poly1305 unit tests ------------------------------------------------

    #[test]
    fn poly1305_known_vector() {
        // RFC 8439 Section 2.5.2 test vector
        let msg = b"Cryptographic Forum Research Group";
        let key: [u8; 32] = [
            0x85, 0xd6, 0xbe, 0x78, 0x57, 0x55, 0x6d, 0x33, 0x7f, 0x44, 0x52, 0xfe, 0x42, 0xd5,
            0x06, 0xa8, 0x01, 0x03, 0x80, 0x8a, 0xfb, 0x0d, 0xb2, 0xfd, 0x4a, 0xbf, 0xf6, 0xaf,
            0x41, 0x49, 0xf5, 0x1b,
        ];
        let expected: [u8; 16] = [
            0xa8, 0x06, 0x1d, 0xc1, 0x30, 0x51, 0x36, 0xc6, 0xc2, 0x2b, 0x8b, 0xaf, 0x0c, 0x01,
            0x27, 0xa9,
        ];
        let tag = poly1305_mac(msg, &key);
        assert_eq!(tag, expected);
    }

    // -- AEAD encrypt/decrypt tests -----------------------------------------

    #[test]
    fn encrypt_decrypt_buffer_roundtrip() {
        let config = test_config();
        let plaintext = b"Hello, ExchangeDB encryption!";

        let encrypted = encrypt_buffer(plaintext, &config).unwrap();
        assert_ne!(&encrypted[..], &plaintext[..]);
        assert!(encrypted.starts_with(MAGIC));

        let decrypted = decrypt_buffer(&encrypted, &config).unwrap();
        assert_eq!(&decrypted, plaintext);
    }

    #[test]
    fn encrypt_decrypt_empty_data() {
        let config = test_config();
        let plaintext = b"";
        let encrypted = encrypt_buffer(plaintext, &config).unwrap();
        let decrypted = decrypt_buffer(&encrypted, &config).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn encrypt_decrypt_large_data() {
        let config = test_config();
        let plaintext: Vec<u8> = (0..10_000).map(|i| (i % 256) as u8).collect();
        let encrypted = encrypt_buffer(&plaintext, &config).unwrap();
        let decrypted = decrypt_buffer(&encrypted, &config).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn encrypt_decrypt_1mb_data() {
        let config = test_config();
        let plaintext: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();
        let encrypted = encrypt_buffer(&plaintext, &config).unwrap();
        let decrypted = decrypt_buffer(&encrypted, &config).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn encrypt_decrypt_file_roundtrip() {
        let config = test_config();
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("column.dat");

        let plaintext = b"column data for encryption test";
        fs::write(&file_path, plaintext).unwrap();

        encrypt_file(&file_path, &config).unwrap();

        // Encrypted file should exist
        let enc_path = encrypted_path(&file_path);
        assert!(enc_path.exists());

        // Remove original so we confirm decrypt actually restores it
        fs::remove_file(&file_path).unwrap();
        assert!(!file_path.exists());

        decrypt_file(&file_path, &config).unwrap();
        let restored = fs::read(&file_path).unwrap();
        assert_eq!(&restored, plaintext);
    }

    #[test]
    fn disabled_config_passes_through() {
        let config = EncryptionConfig::disabled();
        let data = b"should not be encrypted";
        let result = encrypt_buffer(data, &config).unwrap();
        assert_eq!(&result, data);
    }

    #[test]
    fn wrong_key_fails_authentication() {
        let config = test_config();
        let plaintext = b"secret data";
        let encrypted = encrypt_buffer(plaintext, &config).unwrap();

        let wrong_config = EncryptionConfig::new(
            EncryptionAlgorithm::ChaCha20Poly1305,
            vec![0xCD; 32],
        )
        .unwrap();
        // With authenticated encryption, wrong key should produce an error, not wrong plaintext
        let result = decrypt_buffer(&encrypted, &wrong_config);
        assert!(result.is_err());
    }

    #[test]
    fn tampered_ciphertext_detected() {
        let config = test_config();
        let plaintext = b"important data";
        let mut encrypted = encrypt_buffer(plaintext, &config).unwrap();

        // Tamper with a ciphertext byte
        if encrypted.len() > HEADER_SIZE + 1 {
            encrypted[HEADER_SIZE + 1] ^= 0xFF;
        }

        let result = decrypt_buffer(&encrypted, &config);
        assert!(result.is_err());
    }

    #[test]
    fn tampered_tag_detected() {
        let config = test_config();
        let plaintext = b"important data";
        let mut encrypted = encrypt_buffer(plaintext, &config).unwrap();

        // Tamper with a tag byte (at offset 17..33)
        encrypted[20] ^= 0x01;

        let result = decrypt_buffer(&encrypted, &config);
        assert!(result.is_err());
    }

    #[test]
    fn invalid_key_length_rejected() {
        let result = EncryptionConfig::new(EncryptionAlgorithm::ChaCha20Poly1305, vec![0; 16]);
        assert!(result.is_err());
    }

    #[test]
    fn header_contains_algorithm() {
        let config = test_config();
        let encrypted = encrypt_buffer(b"test", &config).unwrap();
        assert_eq!(encrypted[4], 3); // ChaCha20Poly1305 = 3
    }

    #[test]
    fn gcm_algorithm_roundtrip() {
        let config = gcm_config();
        let data = b"GCM mode test data";
        let enc = encrypt_buffer(data, &config).unwrap();
        let dec = decrypt_buffer(&enc, &config).unwrap();
        assert_eq!(&dec, data);
    }

    #[test]
    fn legacy_cbc_roundtrip() {
        // Legacy XOR path still works
        let config = legacy_config();
        let data = b"Legacy CBC test";
        let enc = encrypt_buffer(data, &config).unwrap();
        let dec = decrypt_buffer(&enc, &config).unwrap();
        assert_eq!(&dec, data);
    }

    #[test]
    fn different_keys_produce_different_ciphertext() {
        let c1 = EncryptionConfig::new(EncryptionAlgorithm::ChaCha20Poly1305, vec![0xAA; 32])
            .unwrap();
        let c2 = EncryptionConfig::new(EncryptionAlgorithm::ChaCha20Poly1305, vec![0xBB; 32])
            .unwrap();
        let plain = b"test data";
        let e1 = encrypt_buffer(plain, &c1).unwrap();
        let e2 = encrypt_buffer(plain, &c2).unwrap();
        // Different keys => different ciphertext (excluding header randomness)
        // Even with random nonces, the ciphertext bodies will differ
        assert_ne!(e1, e2);
    }

    #[test]
    fn nonce_is_random_each_time() {
        let config = test_config();
        let enc1 = encrypt_buffer(b"same", &config).unwrap();
        let enc2 = encrypt_buffer(b"same", &config).unwrap();
        // Random nonce => different ciphertext each time
        assert_ne!(enc1, enc2);
    }

    #[test]
    fn all_zeros_plaintext() {
        let config = test_config();
        let plain = vec![0u8; 1024];
        let enc = encrypt_buffer(&plain, &config).unwrap();
        let dec = decrypt_buffer(&enc, &config).unwrap();
        assert_eq!(dec, plain);
    }

    #[test]
    fn all_0xff_plaintext() {
        let config = test_config();
        let plain = vec![0xFF; 1024];
        let enc = encrypt_buffer(&plain, &config).unwrap();
        let dec = decrypt_buffer(&enc, &config).unwrap();
        assert_eq!(dec, plain);
    }

    #[test]
    fn both_new_algorithms_work() {
        for algo in [
            EncryptionAlgorithm::Aes256Gcm,
            EncryptionAlgorithm::ChaCha20Poly1305,
        ] {
            let config = EncryptionConfig::new(algo, vec![0x42; 32]).unwrap();
            let data = b"algorithm test";
            let enc = encrypt_buffer(data, &config).unwrap();
            let dec = decrypt_buffer(&enc, &config).unwrap();
            assert_eq!(&dec, data);
        }
    }

    #[test]
    fn ciphertext_longer_than_plaintext() {
        let config = test_config();
        let plain = b"data";
        let enc = encrypt_buffer(plain, &config).unwrap();
        // Header (33 bytes) + ciphertext (same length as plaintext)
        assert_eq!(enc.len(), HEADER_SIZE + plain.len());
    }

    #[test]
    fn decrypt_truncated_header_is_error() {
        let config = test_config();
        let result = decrypt_buffer(b"XEN", &config);
        assert!(result.is_err());
    }

    #[test]
    fn decrypt_wrong_magic_is_error() {
        let config = test_config();
        let mut bad = vec![0; 40];
        bad[0..4].copy_from_slice(b"BAAD");
        let result = decrypt_buffer(&bad, &config);
        assert!(result.is_err());
    }

    #[test]
    fn decrypt_unknown_algorithm_byte_is_error() {
        let config = test_config();
        let mut bad = vec![0; 40];
        bad[0..4].copy_from_slice(b"XENC");
        bad[4] = 99; // unknown algo
        let result = decrypt_buffer(&bad, &config);
        assert!(result.is_err());
    }

    #[test]
    fn single_byte_data() {
        let config = test_config();
        let enc = encrypt_buffer(b"X", &config).unwrap();
        let dec = decrypt_buffer(&enc, &config).unwrap();
        assert_eq!(dec, b"X");
    }
}
