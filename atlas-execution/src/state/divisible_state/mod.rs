
use std::sync::{Arc, Weak};

#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};
use atlas_common::error::*;
use atlas_common::crypto::hash::Digest;
use atlas_common::ordering::{Orderable, SeqNo};

pub enum InstallStateMessage<S> where S: DivisibleState {
    /// We have received a part of the state
    StatePart(Vec<S::StatePart>),
    /// We can go back to polling the regular channel for new messages, as we are done installing state
    Done
}

/// The message that is sent when a checkpoint is done by the execution module
/// and a state must be returned for the state transfer protocol
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone,Debug)]
pub struct AppStateMessage<S> where S: DivisibleState {
    seq_no: SeqNo,
    state_descriptor: S::StateDescriptor,
    altered_parts: Vec<S::StatePart>,

}

/// The trait that represents the ID of a part
pub trait PartId: PartialEq + PartialOrd + Clone {

    fn content_description(&self) -> &[u8];
    fn seq_no(&self) -> &SeqNo;

}

/// The abstraction for a divisible state, to be used by the state transfer protocol
pub trait DivisibleStateDescriptor<S: DivisibleState + ?Sized>: Orderable + PartialEq + Clone + Send {

    /// Get all the parts of the state
    fn parts(&self) -> Box<[Arc<S::PartDescription>]>;

    /// Compare two states
    //fn compare_descriptors(&self, other: &Self) -> Vec<S::PartDescription>;

    fn get_digest(&self) -> Option<Digest>;

}


/// A part of the state
pub trait StatePart<S: DivisibleState + ?Sized> {

    fn descriptor(&self) -> &S::PartDescription;

    fn hash(&self) -> Digest;

    fn id(&self) -> &[u8];

    fn length(&self) -> usize;

    fn bytes(&self) -> &[u8];

}

pub trait PartDescription {
    fn id(&self) -> &[u8];
}

///
/// The trait that represents a divisible state, to be used by the state transfer protocol
///
pub trait DivisibleState: Sized + Clone + Send + Sync {
    #[cfg(feature = "serialize_serde")]
    type PartDescription: PartDescription + PartId + for<'a> Deserialize<'a> + Serialize + Send + Clone + std::fmt::Debug;

    #[cfg(feature = "serialize_capnp")]
    type PartDescription: PartDescription + PartId + Send + Clone;

    #[cfg(feature = "serialize_serde")]
    type StateDescriptor: DivisibleStateDescriptor<Self> + for<'a> Deserialize<'a> + Serialize + Send + Clone + std::fmt::Debug;

    #[cfg(feature = "serialize_capnp")]
    type StateDescriptor: DivisibleStateDescriptor<Self> + Send + Clone;
    
    #[cfg(feature = "serialize_serde")]
    type StatePart: StatePart<Self> + for<'a> Deserialize<'a> + Serialize + Send + Clone;

    #[cfg(feature = "serialize_capnp")]
    type StatePart: StatePart<Self> + Send + Clone;

    fn get_descriptor(&self) -> Self::StateDescriptor;

    /// Accept a number of parts into our current state
    fn accept_parts(&mut self, parts: Vec<Self::StatePart>) -> Result<()>;

    // Here we should perform any checks to see if the database is valid
    fn finalize_transfer(&mut self) -> Result<()>;

    /// Get the parts corresponding to the provided part descriptions
    fn get_parts(&mut self) -> Result<(Vec<Self::StatePart>,Self::StateDescriptor)>;

    fn get_seqno(&self) -> Result<SeqNo>;
}

impl<S> AppStateMessage<S> where S: DivisibleState {

    //Constructor
    pub fn new(seq_no: SeqNo, altered_parts: Vec<S::StatePart>, state_descriptor:S::StateDescriptor) -> Self {
        AppStateMessage {
            seq_no,
            state_descriptor,
            altered_parts,
        }
    }

    pub fn into_state(self) -> (SeqNo,Vec<S::StatePart>,S::StateDescriptor) {
        (self.seq_no,self.altered_parts,self.state_descriptor)
    }

}

impl<S> Orderable for AppStateMessage<S> where S: DivisibleState {
    fn sequence_number(&self) -> SeqNo {
        self.seq_no
    }
}
