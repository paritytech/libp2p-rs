extern crate asn1;

use std::fmt;

pub enum Modulo {
    I2048([u8; 256]),
    I1024([u8; 128]),
}

impl fmt::Debug for Modulo {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        use Modulo as RealModulo;

        #[derive(Debug)]
        enum Modulo<'a> {
            I2048(&'a [u8]),
            I1024(&'a [u8]),
        }

        write!(
            f,
            "{:?}",
            match *self {
                RealModulo::I1024(ref arr) => Modulo::I1024(arr),
                RealModulo::I2048(ref arr) => Modulo::I2048(arr),
            }
        )
    }
}

impl PartialEq for Modulo {
    fn eq(&self, other: &Self) -> bool {
        use Modulo as RealModulo;

        #[derive(PartialEq, Eq)]
        enum Modulo<'a> {
            I2048(&'a [u8]),
            I1024(&'a [u8]),
        }

        let from = |real| match real {
            &RealModulo::I1024(ref arr) => Modulo::I1024(arr),
            &RealModulo::I2048(ref arr) => Modulo::I2048(arr),
        };

        from(self).eq(&from(other))
    }
}

impl Eq for Modulo {}

#[derive(Debug, PartialEq, Eq)]
pub struct RSAPublicKey {
    pub modulo: Modulo,
    pub exponent: u64,
}

impl RSAPublicKey {
    pub fn is_1024(&self) -> bool {
        if let Modulo::I1024(_) = self.modulo {
            true
        } else {
            false
        }
    }

    pub fn is_2048(&self) -> bool {
        if let Modulo::I2048(_) = self.modulo {
            true
        } else {
            false
        }
    }

    pub fn parse(data: &[u8]) -> asn1::DeserializationResult<Self> {
        asn1::deserialize(data, |d| {
            d.read_sequence(|d| {
                d.read_sequence(|d| {
                    let obj_id = d.read_object_identifier()?;
                    if &obj_id.parts != &[1, 2, 840, 113549, 1, 1, 1] {
                        Err(asn1::DeserializationError::InvalidValue)
                    } else {
                        // Ignore the algorithm-specific data
                        d.ignore()?;

                        Ok(())
                    }
                })?;

                d.read_bit_string().and_then(|bitstring| {
                    asn1::deserialize(bitstring.as_bytes(), |d| {
                        d.read_sequence(|d| {
                            let modulo = d.read_int_bytes::<[u8; 256]>()
                                .map(Modulo::I2048)
                                .or_else(|_| d.read_int_bytes::<[u8; 128]>().map(Modulo::I1024))?;

                            let exponent = d.read_int()?;

                            Ok(RSAPublicKey { modulo, exponent })
                        })
                    })
                })
            })
        })
    }
}

#[cfg(test)]
mod tests {
    extern crate base64;

    use super::{Modulo, RSAPublicKey};

    #[test]
    fn parse_rsa_1024() {
        let bytes = self::base64::decode(
            "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC4eAm6RBpsXSmynQ/iJtckViTKobjqMLZegz4x\
             EfkCTf5VRKsQRfSqaQU58y0rPn8A0V1aHB+2FEmIS4k/1pTRxhFsjgichOPsM+EjXHqn7sYC2mvh\
             VipgpKzGuHXVLLDm9XAmv/u8SZheVNx9hvrCWgjhe5arO/2v/Lf3MayDpwIDAQAB",
        ).unwrap();

        assert_eq!(
            RSAPublicKey::parse(&bytes),
            Ok(RSAPublicKey {
                modulo: Modulo::I1024([
                    0xB8, 0x78, 0x09, 0xBA, 0x44, 0x1A, 0x6C, 0x5D, 0x29, 0xB2, 0x9D, 0x0F, 0xE2,
                    0x26, 0xD7, 0x24, 0x56, 0x24, 0xCA, 0xA1, 0xB8, 0xEA, 0x30, 0xB6, 0x5E, 0x83,
                    0x3E, 0x31, 0x11, 0xF9, 0x02, 0x4D, 0xFE, 0x55, 0x44, 0xAB, 0x10, 0x45, 0xF4,
                    0xAA, 0x69, 0x05, 0x39, 0xF3, 0x2D, 0x2B, 0x3E, 0x7F, 0x00, 0xD1, 0x5D, 0x5A,
                    0x1C, 0x1F, 0xB6, 0x14, 0x49, 0x88, 0x4B, 0x89, 0x3F, 0xD6, 0x94, 0xD1, 0xC6,
                    0x11, 0x6C, 0x8E, 0x08, 0x9C, 0x84, 0xE3, 0xEC, 0x33, 0xE1, 0x23, 0x5C, 0x7A,
                    0xA7, 0xEE, 0xC6, 0x02, 0xDA, 0x6B, 0xE1, 0x56, 0x2A, 0x60, 0xA4, 0xAC, 0xC6,
                    0xB8, 0x75, 0xD5, 0x2C, 0xB0, 0xE6, 0xF5, 0x70, 0x26, 0xBF, 0xFB, 0xBC, 0x49,
                    0x98, 0x5E, 0x54, 0xDC, 0x7D, 0x86, 0xFA, 0xC2, 0x5A, 0x08, 0xE1, 0x7B, 0x96,
                    0xAB, 0x3B, 0xFD, 0xAF, 0xFC, 0xB7, 0xF7, 0x31, 0xAC, 0x83, 0xA7,
                ]),
                exponent: 65537,
            })
        )
    }

    #[test]
    fn parse_rsa_2048() {
        let bytes = self::base64::decode(
            "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAw3Dq8sdKZ/Lq0AkCFRB+ywQXpubvHgsc\
             R+WyVV4tdQE+0OcJSC5hx5W+XLR/y21PTe/30f0oYP7oJv8rH2Mov1Gvops2l6efVqPA8ZggDRrA\
             kotjLXXJggDimIGichRS9+izNi/Lit77H2bFLmlkTfrFOjibWrPP+XvoYRFN3B1gyUT5P1hARePl\
             bb86dcd1e5l/x/lBDH7DJ+TxsY7li6HjgvlxK4jAXa9yzdkDvJOpScs+la7gGawwesDKoQ5dWyql\
             gT93cbXhwOHTUvownl0hwtYjiK9UGWW8ptn9/3ehYAyi6Kx/SqLJsXiJFlPg16KNunGBHL7VAFyY\
             Z51NEwIDAQAB"
        ).unwrap();

        assert_eq!(
            RSAPublicKey::parse(&bytes),
            Ok(RSAPublicKey {
                modulo: Modulo::I2048([
                    0xC3, 0x70, 0xEA, 0xF2, 0xC7, 0x4A, 0x67, 0xF2, 0xEA, 0xD0, 0x09, 0x02, 0x15,
                    0x10, 0x7E, 0xCB, 0x04, 0x17, 0xA6, 0xE6, 0xEF, 0x1E, 0x0B, 0x1C, 0x47, 0xE5,
                    0xB2, 0x55, 0x5E, 0x2D, 0x75, 0x01, 0x3E, 0xD0, 0xE7, 0x09, 0x48, 0x2E, 0x61,
                    0xC7, 0x95, 0xBE, 0x5C, 0xB4, 0x7F, 0xCB, 0x6D, 0x4F, 0x4D, 0xEF, 0xF7, 0xD1,
                    0xFD, 0x28, 0x60, 0xFE, 0xE8, 0x26, 0xFF, 0x2B, 0x1F, 0x63, 0x28, 0xBF, 0x51,
                    0xAF, 0xA2, 0x9B, 0x36, 0x97, 0xA7, 0x9F, 0x56, 0xA3, 0xC0, 0xF1, 0x98, 0x20,
                    0x0D, 0x1A, 0xC0, 0x92, 0x8B, 0x63, 0x2D, 0x75, 0xC9, 0x82, 0x00, 0xE2, 0x98,
                    0x81, 0xA2, 0x72, 0x14, 0x52, 0xF7, 0xE8, 0xB3, 0x36, 0x2F, 0xCB, 0x8A, 0xDE,
                    0xFB, 0x1F, 0x66, 0xC5, 0x2E, 0x69, 0x64, 0x4D, 0xFA, 0xC5, 0x3A, 0x38, 0x9B,
                    0x5A, 0xB3, 0xCF, 0xF9, 0x7B, 0xE8, 0x61, 0x11, 0x4D, 0xDC, 0x1D, 0x60, 0xC9,
                    0x44, 0xF9, 0x3F, 0x58, 0x40, 0x45, 0xE3, 0xE5, 0x6D, 0xBF, 0x3A, 0x75, 0xC7,
                    0x75, 0x7B, 0x99, 0x7F, 0xC7, 0xF9, 0x41, 0x0C, 0x7E, 0xC3, 0x27, 0xE4, 0xF1,
                    0xB1, 0x8E, 0xE5, 0x8B, 0xA1, 0xE3, 0x82, 0xF9, 0x71, 0x2B, 0x88, 0xC0, 0x5D,
                    0xAF, 0x72, 0xCD, 0xD9, 0x03, 0xBC, 0x93, 0xA9, 0x49, 0xCB, 0x3E, 0x95, 0xAE,
                    0xE0, 0x19, 0xAC, 0x30, 0x7A, 0xC0, 0xCA, 0xA1, 0x0E, 0x5D, 0x5B, 0x2A, 0xA5,
                    0x81, 0x3F, 0x77, 0x71, 0xB5, 0xE1, 0xC0, 0xE1, 0xD3, 0x52, 0xFA, 0x30, 0x9E,
                    0x5D, 0x21, 0xC2, 0xD6, 0x23, 0x88, 0xAF, 0x54, 0x19, 0x65, 0xBC, 0xA6, 0xD9,
                    0xFD, 0xFF, 0x77, 0xA1, 0x60, 0x0C, 0xA2, 0xE8, 0xAC, 0x7F, 0x4A, 0xA2, 0xC9,
                    0xB1, 0x78, 0x89, 0x16, 0x53, 0xE0, 0xD7, 0xA2, 0x8D, 0xBA, 0x71, 0x81, 0x1C,
                    0xBE, 0xD5, 0x00, 0x5C, 0x98, 0x67, 0x9D, 0x4D, 0x13,
                ]),
                exponent: 65537,
            })
        )
    }
}
