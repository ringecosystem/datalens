use datalens_core::{DatalensError, DatalensErrorKind, EvmLogFilter, TopicFilter};
use sha3::{Digest, Keccak256};

const BLOOM_BYTES: usize = 256;
#[cfg(test)]
const BLOOM_BITS: usize = BLOOM_BYTES * 8;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvmLogBloom {
    bytes: [u8; BLOOM_BYTES],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvmLogBloomInput<'a> {
    Address(&'a str),
    Topic(&'a str),
}

impl EvmLogBloom {
    pub fn from_hex(value: impl AsRef<str>) -> Result<Self, DatalensError> {
        let bytes = parse_hex_bytes("logsBloom", value.as_ref(), BLOOM_BYTES)?;
        let mut bloom = [0_u8; BLOOM_BYTES];
        bloom.copy_from_slice(&bytes);
        Ok(Self { bytes: bloom })
    }

    pub fn from_inputs<'a>(
        inputs: impl IntoIterator<Item = EvmLogBloomInput<'a>>,
    ) -> Result<Self, DatalensError> {
        let mut bloom = Self {
            bytes: [0_u8; BLOOM_BYTES],
        };
        for input in inputs {
            match input {
                EvmLogBloomInput::Address(value) => {
                    bloom.insert(&parse_hex_bytes("address", value, 20)?);
                }
                EvmLogBloomInput::Topic(value) => {
                    bloom.insert(&parse_hex_bytes("topic", value, 32)?);
                }
            }
        }
        Ok(bloom)
    }

    pub fn as_hex(&self) -> String {
        let mut value = String::with_capacity(2 + BLOOM_BYTES * 2);
        value.push_str("0x");
        for byte in self.bytes {
            value.push_str(&format!("{byte:02x}"));
        }
        value
    }

    pub fn may_contain_address(&self, address: &str) -> Result<bool, DatalensError> {
        self.may_contain_bytes(&parse_hex_bytes("address", address, 20)?)
    }

    pub fn may_contain_topic(&self, topic: &str) -> Result<bool, DatalensError> {
        self.may_contain_bytes(&parse_hex_bytes("topic", topic, 32)?)
    }

    pub fn may_match_filter(&self, filter: &EvmLogFilter) -> Result<bool, DatalensError> {
        if !filter.addresses().is_empty()
            && !filter
                .addresses()
                .iter()
                .map(|address| self.may_contain_address(address))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .any(|matches| matches)
        {
            return Ok(false);
        }

        for topic in filter.topics() {
            match topic {
                TopicFilter::Wildcard => {}
                TopicFilter::AnyOf(values) => {
                    if values.is_empty() {
                        return Ok(false);
                    }
                    let mut slot_matches = false;
                    for value in values {
                        if self.may_contain_topic(value)? {
                            slot_matches = true;
                            break;
                        }
                    }
                    if !slot_matches {
                        return Ok(false);
                    }
                }
            }
        }

        Ok(true)
    }

    fn insert(&mut self, value: &[u8]) {
        for index in bloom_indexes(value) {
            set_bit(&mut self.bytes, index);
        }
    }

    fn may_contain_bytes(&self, value: &[u8]) -> Result<bool, DatalensError> {
        Ok(bloom_indexes(value)
            .into_iter()
            .all(|index| bit_is_set(&self.bytes, index)))
    }
}

fn bloom_indexes(value: &[u8]) -> [usize; 3] {
    let digest = Keccak256::digest(value);
    [
        bloom_index(digest[0], digest[1]),
        bloom_index(digest[2], digest[3]),
        bloom_index(digest[4], digest[5]),
    ]
}

fn bloom_index(high: u8, low: u8) -> usize {
    usize::from(u16::from_be_bytes([high, low]) & 0x07ff)
}

fn set_bit(bytes: &mut [u8; BLOOM_BYTES], index: usize) {
    bytes[bloom_byte_index(index)] |= bloom_bit_mask(index);
}

fn bit_is_set(bytes: &[u8; BLOOM_BYTES], index: usize) -> bool {
    bytes[bloom_byte_index(index)] & bloom_bit_mask(index) != 0
}

fn bloom_byte_index(index: usize) -> usize {
    BLOOM_BYTES - 1 - (index / 8)
}

fn bloom_bit_mask(index: usize) -> u8 {
    1 << (index % 8)
}

fn parse_hex_bytes(
    field: &str,
    value: &str,
    expected_len: usize,
) -> Result<Vec<u8>, DatalensError> {
    let value = value.trim();
    let value = value.strip_prefix("0x").unwrap_or(value);
    if value.len() != expected_len * 2 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("{field} must be {expected_len} bytes"),
        ));
    }
    if value.len() % 2 != 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("{field} must have an even number of hex digits"),
        ));
    }

    (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!("invalid hex {field}: {error}"),
                )
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_indexes_are_11_bit_values() {
        for index in bloom_indexes(&[0xab; 20]) {
            assert!(index < BLOOM_BITS);
        }
    }
}
