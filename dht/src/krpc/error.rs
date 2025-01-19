use serde::{de::Visitor, ser::SerializeSeq, Deserializer, Serializer};
use serde_bytes::ByteBuf;

use super::{protocol::KrpcPacket, Rpc};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Protocol(#[from] KrpcError),
    #[error("Query timed out")]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("Failed encoding packet")]
    PacketEncoding(#[from] serde_bencoded::SerError),
    #[error("Socket error")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, PartialEq)]
pub struct KrpcError {
    pub code: u64,
    pub description: String,
}

impl KrpcError {
    pub fn generic(description: String) -> Self {
        Self {
            code: 201,
            description,
        }
    }

    #[allow(dead_code)]
    pub fn server(description: String) -> Self {
        Self {
            code: 202,
            description,
        }
    }

    pub fn protocol(description: String) -> Self {
        Self {
            code: 203,
            description,
        }
    }

    pub fn method_unknown(description: String) -> Self {
        Self {
            code: 204,
            description,
        }
    }
}

impl Rpc for KrpcError {
    type Response = ();

    fn into_packet(self, transaction_id: ByteBuf) -> KrpcPacket {
        KrpcPacket {
            t: transaction_id,
            y: 'e',
            q: None,
            a: None,
            r: None,
            e: Some(self),
        }
    }
}

pub(crate) fn deserialize_error<'de, D>(de: D) -> Result<Option<KrpcError>, D::Error>
where
    D: Deserializer<'de>,
{
    struct ErrorVisitor;

    impl<'de> Visitor<'de> for ErrorVisitor {
        type Value = Option<KrpcError>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("Expected bencoded list with one error code + description")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let Some(code) = seq.next_element::<u64>()? else {
                return Ok(None);
            };
            let Some(description) = seq.next_element::<String>()? else {
                return Ok(None);
            };
            // The next_element must be called here for parsing of the rest of the
            // message to be successful for whatever reason. **shrug**
            if matches!(seq.next_element::<char>(), Ok(None)) {
                Ok(Some(KrpcError { code, description }))
            } else {
                Ok(None)
            }
        }
    }
    de.deserialize_any(ErrorVisitor)
}

pub(crate) fn serialize_error<S>(error: &Option<KrpcError>, se: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let Some(KrpcError { code, description }) = error else {
        return se.serialize_none();
    };

    let mut seq = se.serialize_seq(Some(2))?;
    seq.serialize_element::<u64>(code)?;
    seq.serialize_element::<String>(description)?;
    seq.end()
}

impl std::fmt::Display for KrpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "KrpcError: {},  {}",
            self.code, self.description
        ))
    }
}

impl std::error::Error for KrpcError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_generic_error() {
        let encoded = "d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee";
        let decoded: KrpcPacket = serde_bencoded::from_str(encoded).unwrap();
        let expected = KrpcPacket {
            t: ByteBuf::from(b"aa".to_vec()),
            y: 'e',
            r: None,
            e: Some(KrpcError::generic("A Generic Error Ocurred".to_owned())),
            q: None,
            a: None,
        };
        assert_eq!(expected, decoded);
        assert_eq!(encoded, serde_bencoded::to_string(&expected).unwrap());
    }

    #[test]
    fn roundtrip_server_error() {
        let encoded = "d1:eli202e12:Server Errore1:t2:lC1:y1:ee";
        let decoded: KrpcPacket = serde_bencoded::from_str(encoded).unwrap();
        let expected = KrpcPacket {
            t: ByteBuf::from(b"lC".to_vec()),
            y: 'e',
            r: None,
            e: Some(KrpcError::server("Server Error".to_owned())),
            q: None,
            a: None,
        };
        assert_eq!(expected, decoded);
        assert_eq!(encoded, serde_bencoded::to_string(&expected).unwrap());
    }
}
