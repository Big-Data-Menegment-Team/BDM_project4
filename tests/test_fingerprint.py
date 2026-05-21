"""Tests for rico.fingerprint — SHA-256 helpers behind `source_fingerprint`."""

from rico.fingerprint import sha256_bytes, sha256_text


def test_sha256_bytes_matches_known_digest():
    # Reference: printf 'hello' | shasum -a 256
    assert sha256_bytes(b"hello") == (
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    )


def test_sha256_bytes_returns_64_char_lowercase_hex():
    digest = sha256_bytes(b"any payload")
    assert len(digest) == 64
    assert digest == digest.lower()
    int(digest, 16)  # raises ValueError if the digest is not valid hex


def test_sha256_text_equals_sha256_of_utf8_bytes():
    assert sha256_text("hello") == sha256_bytes(b"hello")


def test_sha256_text_handles_non_ascii():
    assert sha256_text("café") == sha256_bytes("café".encode("utf-8"))
