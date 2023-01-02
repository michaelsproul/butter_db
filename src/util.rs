use crate::Error;

pub fn key_from_hex_bytes(hex_bytes: &[u8]) -> Result<Vec<u8>, Error> {
    let mut key = Vec::with_capacity(hex_bytes.len() / 2);
    faster_hex::hex_decode(&hex_bytes, &mut key).map_err(|_| Error::Oops)?;
    Ok(key)
}
