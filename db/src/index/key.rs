//! Index key and entry types shared across every access method.
//!
//! Both B+Tree leaves and hash buckets store [`IndexEntry`] values, and
//! every lookup is expressed as a [`CompositeKey`]. They live here together
//! because they're a tight pair: an entry is just a key plus a record id,
//! and both share the same codec story.
//!
//! Nothing in this module reaches into a specific access method — it's
//! pure data + codec, usable by any future family.

use std::io::{Read, Write};

use crate::{
    Value,
    codec::{CodecError, Decode, Encode},
    primitives::RecordId,
};

/// An index lookup key: one [`Value`] per indexed column, in declaration order.
///
/// Single-column indexes such as `CREATE INDEX … ON t (a)` produce arity-1
/// keys; composite indexes such as `CREATE INDEX … ON t (a, b)` produce
/// arity-2 keys. Arity is fixed at index-creation time and recorded in the
/// catalog alongside the column types — every lookup must present a key of
/// matching arity and per-position type.
///
/// The derived `Hash` and `PartialOrd` impls give us, respectively, a
/// length-prefixed mix of every component (so `("a", "bc")` and `("ab", "c")`
/// hash differently) and lexicographic ordering by component (which is what
/// the B-tree wants for range scans).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct CompositeKey(Vec<Value>);

impl CompositeKey {
    /// Builds a composite key from the given components, in column order.
    pub fn new(values: Vec<Value>) -> Self {
        Self(values)
    }

    /// Convenience for the single-column case.
    pub fn single(v: Value) -> Self {
        Self(vec![v])
    }

    /// Number of indexed columns this key carries.
    pub fn arity(&self) -> usize {
        self.0.len()
    }

    /// The components in declaration order.
    pub fn components(&self) -> &[Value] {
        &self.0
    }

    /// Bytes the key occupies on disk: a 4-byte arity prefix plus each
    /// component's `encoded_size`. The arity is technically redundant with
    /// the catalog but it makes index pages self-describing for offline tools.
    pub fn encoded_size(&self) -> usize {
        4 + self.0.iter().map(Value::encoded_size).sum::<usize>()
    }
}

impl Encode for CompositeKey {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.0.encode(w)
    }
}

impl Decode for CompositeKey {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        Ok(Self(Vec::decode(r)?))
    }
}

/// A single `(key, rid)` pair — the leaf-level unit of storage in any
/// index.
///
/// Both B+Tree leaves and hash buckets store these. The codec is shared
/// because [`CompositeKey`] and [`RecordId`] both implement [`Encode`] /
/// [`Decode`].
#[derive(Debug, Clone, PartialEq)]
pub struct IndexEntry {
    /// The indexed column values in declaration order.
    pub key: CompositeKey,
    /// The physical row location this key points to.
    pub rid: RecordId,
}

impl IndexEntry {
    /// Builds one index entry from a key and a row pointer.
    pub fn new(key: CompositeKey, rid: RecordId) -> Self {
        Self { key, rid }
    }

    /// Returns the number of bytes this entry occupies when encoded.
    ///
    /// The size is the encoded key size plus the fixed-width encoded
    /// [`RecordId`].
    pub fn encoded_size(&self) -> usize {
        self.key.encoded_size() + RecordId::ENCODED_SIZE
    }
}

impl Encode for IndexEntry {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.key.encode(w)?;
        self.rid.encode(w)
    }
}

impl Decode for IndexEntry {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        let key = CompositeKey::decode(r)?;
        let rid = RecordId::decode(r)?;
        Ok(Self { key, rid })
    }
}

#[cfg(test)]
mod tests {
    //! Direct codec roundtrips for `CompositeKey` and `IndexEntry`. These
    //! types are also exercised transitively through every hash-index test
    //! (each insert/search encodes and decodes through a bucket page), but
    //! a failure there points at the bucket layout — the tests below
    //! localize regressions to the key/entry codec itself.

    use super::*;
    use crate::primitives::{FileId, PageNumber, SlotId};

    fn roundtrip_key(key: &CompositeKey) {
        let mut buf = Vec::new();
        key.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), key.encoded_size());

        let decoded = CompositeKey::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(&decoded, key);
    }

    #[test]
    fn empty_key_roundtrips() {
        // Arity-0 keys aren't useful in practice (every index has at least
        // one column), but the codec shouldn't choke on them — only the
        // 4-byte arity prefix should be written.
        let key = CompositeKey::new(vec![]);
        let mut buf = Vec::new();
        key.encode(&mut buf).unwrap();
        assert_eq!(buf, [0, 0, 0, 0]);
        roundtrip_key(&key);
    }

    #[test]
    fn single_column_key_roundtrips() {
        roundtrip_key(&CompositeKey::single(Value::int32(42)));
        roundtrip_key(&CompositeKey::single(Value::varchar("hello".into())));
    }

    #[test]
    fn multi_column_key_roundtrips() {
        let key = CompositeKey::new(vec![
            Value::int32(7),
            Value::varchar("ada".into()),
            Value::int32(-1),
        ]);
        roundtrip_key(&key);
    }

    #[test]
    fn arity_prefix_is_first_four_bytes_little_endian() {
        // Pin the on-disk shape: the first four bytes must be a u32 LE
        // arity. Index pages depend on this — if the prefix ever changes
        // size or endianness, every existing index file becomes unreadable.
        let key = CompositeKey::new(vec![Value::int32(0); 3]);
        let mut buf = Vec::new();
        key.encode(&mut buf).unwrap();
        assert_eq!(&buf[..4], &[3, 0, 0, 0]);
    }

    #[test]
    fn entry_roundtrips() {
        let rid = RecordId::new(FileId::new(1), PageNumber::new(2), SlotId::new(3).unwrap());
        let entry = IndexEntry::new(
            CompositeKey::new(vec![Value::int32(99), Value::varchar("x".into())]),
            rid,
        );

        let mut buf = Vec::new();
        entry.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), entry.encoded_size());

        let decoded = IndexEntry::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(decoded, entry);
    }
}
