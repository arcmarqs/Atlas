use std::sync::Arc;
use atlas_execution::state::monolithic_state::MonolithicState;
use atlas_common::error::*;
use atlas_common::globals::ReadOnly;
use atlas_communication::Node;
use atlas_execution::serialize::SharedData;
use crate::persistent_log::MonolithicStateLog;
use crate::serialize::{LogTransferMessage, OrderingProtocolMessage, ServiceMsg, StateTransferMessage};
use crate::state_transfer::{Checkpoint, StateTransferProtocol};
use crate::timeouts::Timeouts;

pub trait MonolithicStateTransfer<S, NT, PL>: StateTransferProtocol<S, NT, PL> where S: MonolithicState + 'static,
                                                                                     PL: MonolithicStateLog<S> {

    /// The configuration type the state transfer protocol wants to accept
    type Config;
    
    /// Initialize the state transferring protocol with the given configuration, timeouts and communication layer
    fn initialize(config: Self::Config, timeouts: Timeouts, node: Arc<NT>, log: PL) -> Result<Self>
        where Self: Sized;
    
    /// Handle having received a state from the application
    /// you should also notify the ordering protocol that the state has been received
    /// and processed, so he is now safe to delete the state (Maybe this should be handled by the replica?)
    fn handle_state_received_from_app<D, OP, LP>(&mut self,
                                      state: Arc<ReadOnly<Checkpoint<S>>>) -> Result<()>
        where D: SharedData + 'static,
              OP: OrderingProtocolMessage,
              LP: LogTransferMessage,
              NT: Node<ServiceMsg<D, OP, Self::Serialization, LP>>;

}