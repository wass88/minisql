pub fn copy_null_terminated<const N: usize>(buf: &mut [u8; N], s: &str) {
    let bytes = s.as_bytes();
    let len = std::cmp::min(bytes.len(), N - 1);
    buf[0..len].copy_from_slice(&bytes[0..len]);
    buf[len] = 0;
}
pub fn to_string_null_terminated<const N: usize>(buf: &[u8; N]) -> String {
    let mut len = 0;
    for i in 0..N {
        if buf[i] == 0 {
            len = i;
            break;
        }
    }
    String::from_utf8_lossy(&buf[0..len]).to_string()
}
