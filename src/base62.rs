pub const ALPHABET: &[u8; 62] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

/// Render n in base62 using ALPHABET ("0" for 0).
pub fn encode(mut n: u64) -> String {
    if n == 0 {
        return "0".into();
    }
    let mut buf = [0u8; 11]; // max u64 is 11 base62 digits
    let mut i = buf.len();
    while n > 0 {
        i -= 1;
        buf[i] = ALPHABET[(n % 62) as usize];
        n /= 62;
    }
    // ASCII-only alphabet
    unsafe { std::str::from_utf8_unchecked(&buf[i..]) }.to_string()
}
