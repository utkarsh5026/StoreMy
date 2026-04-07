pub mod heap;

pub const PAGE_SIZE: usize = 4096;
pub const MAX_TUPLE_SIZE: usize = 65535;

pub trait Page: Send + Sync {
    fn page_data(&self) -> [u8; PAGE_SIZE];
    fn before_image(&self) -> Option<[u8; PAGE_SIZE]>;
    fn set_before_image(&mut self);
}

use thiserror::Error;

use crate::primitives::SlotId;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("invalid page size: expected {PAGE_SIZE}, got {got}")]
    InvalidPageSize { got: usize },

    #[error("tuple schema does not match page schema")]
    SchemaMismatch,

    #[error("page is full, no empty slots available")]
    PageFull,

    #[error("tuple size {size} exceeds maximum allowed {max}")]
    TupleTooLarge { size: usize, max: usize },

    #[error("slot index {slot} is out of bounds (page has {num_slots} slots)")]
    SlotOutOfBounds { slot: u16, num_slots: u16 },

    #[error("slot {slot} is already empty")]
    SlotAlreadyEmpty { slot: u16 },

    #[error("tuple has no record id")]
    MissingRecordId,

    #[error("tuple belongs to a different page")]
    WrongPage,

    #[error("failed to parse page data: {0}")]
    ParseError(String),
}

impl StorageError {
    pub fn slot_out_of_bounds(slot: SlotId, num_slots: impl Into<u16>) -> Self {
        Self::SlotOutOfBounds {
            slot: slot.into(),
            num_slots: num_slots.into(),
        }
    }

    pub fn slot_already_empty(slot: SlotId) -> Self {
        Self::SlotAlreadyEmpty { slot: slot.into() }
    }

    pub fn invalid_page_size(got: usize) -> Self {
        Self::InvalidPageSize { got }
    }

    pub fn tuple_too_large(size: usize) -> Self {
        Self::TupleTooLarge {
            size,
            max: MAX_TUPLE_SIZE,
        }
    }
}
