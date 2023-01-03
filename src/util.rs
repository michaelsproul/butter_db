use crate::Error;

pub fn key_from_hex_bytes(hex_bytes: &[u8]) -> Result<Vec<u8>, Error> {
    let mut key = vec![0; hex_bytes.len() / 2];
    faster_hex::hex_decode(&hex_bytes, &mut key).map_err(|_| Error::Oops)?;
    Ok(key)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_key_from_hex_bytes() {
        assert_eq!(key_from_hex_bytes(b"00").unwrap(), vec![0]);
    }
}
