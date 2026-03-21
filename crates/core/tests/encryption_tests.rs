//! Comprehensive encryption tests (40+ tests).
//!
//! Covers buffer encrypt/decrypt, key sizes, wrong keys, empty/large data,
//! file encryption, .enc extension, corruption detection, and authenticated
//! encryption (ChaCha20-Poly1305).

use exchange_core::encryption::{
    decrypt_buffer, decrypt_file, encrypt_buffer, encrypt_file,
    EncryptionAlgorithm, EncryptionConfig,
};
use std::fs;
use tempfile::TempDir;

fn test_config() -> EncryptionConfig {
    EncryptionConfig::new(EncryptionAlgorithm::ChaCha20Poly1305, vec![0xAB; 32]).unwrap()
}

fn config_with_key(key: u8) -> EncryptionConfig {
    EncryptionConfig::new(EncryptionAlgorithm::ChaCha20Poly1305, vec![key; 32]).unwrap()
}

fn legacy_config() -> EncryptionConfig {
    EncryptionConfig::new(EncryptionAlgorithm::Aes256Cbc, vec![0xAB; 32]).unwrap()
}

fn legacy_config_with_key(key: u8) -> EncryptionConfig {
    EncryptionConfig::new(EncryptionAlgorithm::Aes256Cbc, vec![key; 32]).unwrap()
}

// ---------------------------------------------------------------------------
// mod buffer
// ---------------------------------------------------------------------------

mod buffer {
    use super::*;

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let config = test_config();
        let plain = b"Hello, ExchangeDB encryption!";
        let enc = encrypt_buffer(plain, &config).unwrap();
        assert_ne!(&enc[..], &plain[..]);
        let dec = decrypt_buffer(&enc, &config).unwrap();
        assert_eq!(&dec, plain);
    }

    #[test]
    fn empty_data_roundtrip() {
        let config = test_config();
        let enc = encrypt_buffer(b"", &config).unwrap();
        let dec = decrypt_buffer(&enc, &config).unwrap();
        assert!(dec.is_empty());
    }

    #[test]
    fn single_byte_data() {
        let config = test_config();
        let enc = encrypt_buffer(b"X", &config).unwrap();
        let dec = decrypt_buffer(&enc, &config).unwrap();
        assert_eq!(dec, b"X");
    }

    #[test]
    fn large_data_1mb() {
        let config = test_config();
        let plain: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();
        let enc = encrypt_buffer(&plain, &config).unwrap();
        let dec = decrypt_buffer(&enc, &config).unwrap();
        assert_eq!(dec, plain);
    }

    #[test]
    fn large_data_10k() {
        let config = test_config();
        let plain: Vec<u8> = (0..10_000).map(|i| (i % 256) as u8).collect();
        let enc = encrypt_buffer(&plain, &config).unwrap();
        let dec = decrypt_buffer(&enc, &config).unwrap();
        assert_eq!(dec, plain);
    }

    #[test]
    fn wrong_key_fails_authentication() {
        let config = test_config();
        let plain = b"secret data";
        let enc = encrypt_buffer(plain, &config).unwrap();
        let wrong = config_with_key(0xCD);
        // Authenticated encryption: wrong key produces an error, not garbage
        let result = decrypt_buffer(&enc, &wrong);
        assert!(result.is_err());
    }

    #[test]
    fn different_keys_produce_different_ciphertext() {
        let c1 = config_with_key(0xAA);
        let c2 = config_with_key(0xBB);
        let plain = b"test data";
        let e1 = encrypt_buffer(plain, &c1).unwrap();
        let e2 = encrypt_buffer(plain, &c2).unwrap();
        assert_ne!(e1, e2);
    }

    #[test]
    fn disabled_config_passes_through() {
        let config = EncryptionConfig::disabled();
        let data = b"should not be encrypted";
        let result = encrypt_buffer(data, &config).unwrap();
        assert_eq!(&result, data);
    }

    #[test]
    fn disabled_decrypt_passes_through() {
        let config = EncryptionConfig::disabled();
        let data = b"plaintext";
        let result = decrypt_buffer(data, &config).unwrap();
        assert_eq!(&result, data);
    }

    #[test]
    fn encrypted_starts_with_magic() {
        let config = test_config();
        let enc = encrypt_buffer(b"test", &config).unwrap();
        assert_eq!(&enc[0..4], b"XENC");
    }

    #[test]
    fn invalid_key_length_16_rejected() {
        let result = EncryptionConfig::new(EncryptionAlgorithm::ChaCha20Poly1305, vec![0; 16]);
        assert!(result.is_err());
    }

    #[test]
    fn invalid_key_length_0_rejected() {
        let result = EncryptionConfig::new(EncryptionAlgorithm::ChaCha20Poly1305, vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn invalid_key_length_64_rejected() {
        let result = EncryptionConfig::new(EncryptionAlgorithm::ChaCha20Poly1305, vec![0; 64]);
        assert!(result.is_err());
    }

    #[test]
    fn valid_key_length_32_accepted() {
        let result = EncryptionConfig::new(EncryptionAlgorithm::ChaCha20Poly1305, vec![0; 32]);
        assert!(result.is_ok());
    }

    #[test]
    fn aes256_cbc_legacy_roundtrip() {
        let config = legacy_config();
        let plain = b"CBC mode test data";
        let enc = encrypt_buffer(plain, &config).unwrap();
        let dec = decrypt_buffer(&enc, &config).unwrap();
        assert_eq!(&dec, plain);
    }

    #[test]
    fn aes256_gcm_roundtrip() {
        let config =
            EncryptionConfig::new(EncryptionAlgorithm::Aes256Gcm, vec![0x42; 32]).unwrap();
        let plain = b"GCM mode test data";
        let enc = encrypt_buffer(plain, &config).unwrap();
        let dec = decrypt_buffer(&enc, &config).unwrap();
        assert_eq!(&dec, plain);
    }

    #[test]
    fn header_contains_algorithm_cbc() {
        let config = legacy_config();
        let enc = encrypt_buffer(b"test", &config).unwrap();
        assert_eq!(enc[4], 1); // CBC = 1
    }

    #[test]
    fn header_contains_algorithm_gcm() {
        let config =
            EncryptionConfig::new(EncryptionAlgorithm::Aes256Gcm, vec![0x42; 32]).unwrap();
        let enc = encrypt_buffer(b"test", &config).unwrap();
        assert_eq!(enc[4], 2); // GCM = 2
    }

    #[test]
    fn header_contains_algorithm_chacha20() {
        let config = test_config();
        let enc = encrypt_buffer(b"test", &config).unwrap();
        assert_eq!(enc[4], 3); // ChaCha20Poly1305 = 3
    }

    #[test]
    fn nonce_is_random_each_encryption() {
        let config = test_config();
        let enc1 = encrypt_buffer(b"same", &config).unwrap();
        let enc2 = encrypt_buffer(b"same", &config).unwrap();
        // Random nonce means different ciphertext each time (IND-CPA secure)
        assert_ne!(enc1, enc2);
    }

    #[test]
    fn legacy_encrypt_same_data_is_deterministic() {
        let config = legacy_config();
        let enc1 = encrypt_buffer(b"same", &config).unwrap();
        let enc2 = encrypt_buffer(b"same", &config).unwrap();
        // Legacy XOR with deterministic IV => same ciphertext
        assert_eq!(enc1, enc2);
    }

    #[test]
    fn ciphertext_longer_than_plaintext() {
        let config = test_config();
        let plain = b"data";
        let enc = encrypt_buffer(plain, &config).unwrap();
        // Header (33 bytes) + ciphertext (same length as plaintext)
        assert!(enc.len() > plain.len());
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
    fn tampered_ciphertext_detected() {
        let config = test_config();
        let plain = b"important data here";
        let mut enc = encrypt_buffer(plain, &config).unwrap();
        // Tamper with a ciphertext byte after the header
        if enc.len() > 34 {
            enc[34] ^= 0xFF;
        }
        let result = decrypt_buffer(&enc, &config);
        assert!(result.is_err());
    }

    #[test]
    fn tampered_tag_detected() {
        let config = test_config();
        let plain = b"important data here";
        let mut enc = encrypt_buffer(plain, &config).unwrap();
        // Tamper with the Poly1305 tag (bytes 17..33 in header)
        enc[20] ^= 0x01;
        let result = decrypt_buffer(&enc, &config);
        assert!(result.is_err());
    }

    #[test]
    fn legacy_wrong_key_produces_wrong_plaintext() {
        // Legacy XOR does NOT authenticate, so wrong key just gives wrong data
        let config = legacy_config();
        let plain = b"secret data";
        let enc = encrypt_buffer(plain, &config).unwrap();
        let wrong = legacy_config_with_key(0xCD);
        let dec = decrypt_buffer(&enc, &wrong).unwrap();
        assert_ne!(&dec, plain);
    }
}

// ---------------------------------------------------------------------------
// mod file
// ---------------------------------------------------------------------------

mod file {
    use super::*;

    #[test]
    fn encrypt_decrypt_file_roundtrip() {
        let config = test_config();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("column.dat");
        let plain = b"column data for encryption test";
        fs::write(&path, plain).unwrap();

        encrypt_file(&path, &config).unwrap();

        let enc_path = dir.path().join("column.dat.enc");
        assert!(enc_path.exists());

        // Remove original
        fs::remove_file(&path).unwrap();

        decrypt_file(&path, &config).unwrap();
        let restored = fs::read(&path).unwrap();
        assert_eq!(&restored, plain);
    }

    #[test]
    fn enc_extension_added() {
        let config = test_config();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.d");
        fs::write(&path, b"data").unwrap();
        encrypt_file(&path, &config).unwrap();
        assert!(dir.path().join("test.d.enc").exists());
    }

    #[test]
    fn encrypt_disabled_is_noop() {
        let config = EncryptionConfig::disabled();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("noop.d");
        fs::write(&path, b"data").unwrap();
        encrypt_file(&path, &config).unwrap();
        // .enc file should NOT be created when disabled
        assert!(!dir.path().join("noop.d.enc").exists());
    }

    #[test]
    fn corrupt_encrypted_file_detected() {
        let config = test_config();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("col.dat");
        fs::write(&path, b"original data").unwrap();
        encrypt_file(&path, &config).unwrap();

        // Corrupt the encrypted file by truncating it
        let enc_path = dir.path().join("col.dat.enc");
        fs::write(&enc_path, b"XX").unwrap();

        // Remove original
        fs::remove_file(&path).unwrap();

        let result = decrypt_file(&path, &config);
        assert!(result.is_err());
    }

    #[test]
    fn file_with_large_content() {
        let config = test_config();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("large.dat");
        let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        fs::write(&path, &data).unwrap();

        encrypt_file(&path, &config).unwrap();
        fs::remove_file(&path).unwrap();
        decrypt_file(&path, &config).unwrap();

        let restored = fs::read(&path).unwrap();
        assert_eq!(restored, data);
    }

    #[test]
    fn encrypted_file_content_differs_from_original() {
        let config = test_config();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("diff.dat");
        let plain = b"plaintext content";
        fs::write(&path, plain).unwrap();

        encrypt_file(&path, &config).unwrap();

        let enc_content = fs::read(dir.path().join("diff.dat.enc")).unwrap();
        assert_ne!(&enc_content[..], &plain[..]);
    }

    #[test]
    fn encrypt_nonexistent_file_is_error() {
        let config = test_config();
        let result = encrypt_file(std::path::Path::new("/nonexistent/path.dat"), &config);
        assert!(result.is_err());
    }

    #[test]
    fn decrypt_missing_enc_file_is_error() {
        let config = test_config();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("noenc.dat");
        // No .enc file exists
        let result = decrypt_file(&path, &config);
        assert!(result.is_err());
    }

    #[test]
    fn empty_file_roundtrip() {
        let config = test_config();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("empty.dat");
        fs::write(&path, b"").unwrap();
        encrypt_file(&path, &config).unwrap();
        fs::remove_file(&path).unwrap();
        decrypt_file(&path, &config).unwrap();
        let restored = fs::read(&path).unwrap();
        assert!(restored.is_empty());
    }
}
