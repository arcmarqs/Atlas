extern crate core;

use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use atlas_common::channel;
use atlas_common::channel::ChannelSyncTx;
use atlas_common::crypto::hash::Digest;
use atlas_execution::ExecutorHandle;
use atlas_execution::serialize::ApplicationData;
use atlas_common::error::*;
use atlas_common::globals::ReadOnly;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_common::persistentdb::KVDB;
use atlas_communication::message::StoredMessage;
use atlas_core::ordering_protocol::{ProtocolConsensusDecision, ProtocolMessage, SerProof, SerProofMetadata, View};
use atlas_core::persistent_log::{MonolithicStateLog, OrderingProtocolLog, PersistableOrderProtocol, PersistableStateTransferProtocol, StatefulOrderingProtocolLog, WriteMode, DivisibleStateLog};
use atlas_core::serialize::{OrderingProtocolMessage, StatefulOrderProtocolMessage, StateTransferMessage};
use atlas_core::state_transfer::{Checkpoint};
use atlas_core::state_transfer::log_transfer::DecLog;
use atlas_execution::state::divisible_state::DivisibleState;
use atlas_execution::state::monolithic_state::MonolithicState;
use crate::backlog::{ConsensusBacklog, ConsensusBackLogHandle};
use crate::worker::{COLUMN_FAMILY_OTHER, COLUMN_FAMILY_PROOFS, invalidate_seq, PersistentLogWorker, PersistentLogWorkerHandle, PersistentLogWriteStub, read_latest_state, write_latest_seq_no, write_latest_view, write_message, write_proof, write_proof_metadata, write_state};

pub mod serialize;
pub mod backlog;
mod worker;
pub mod metrics;

/// The general type for a callback.
/// Callbacks are optional and can be used when you want to
/// execute a function when the logger stops finishes the computation
// pub type CallbackType = Box<dyn FnOnce(Result<ResponseMessage>) + Send>;
pub type CallbackType = ();

pub enum PersistentLogMode<D: ApplicationData> {
    /// The strict log mode is meant to indicate that the consensus can only be finalized and the
    /// requests executed when the replica has all the information persistently stored.
    ///
    /// This allows for all replicas to crash and still be able to recover from their own stored
    /// local state, meaning we can always recover without losing any piece of replied to information
    /// So we have the guarantee that once a request has been replied to, it will never be lost (given f byzantine faults).
    ///
    /// Performance will be dependent on the speed of the datastore as the consensus will only move to the
    /// executing phase once all requests have been successfully stored.
    Strict(ConsensusBackLogHandle<D::Request>),

    /// Optimistic mode relies a lot more on the assumptions that are made by the BFT algorithm in order
    /// to maximize the performance.
    ///
    /// It works by separating the persistent data storage with the consensus algorithm. It relies on
    /// the fact that we only allow for f faults concurrently, so we assume that we can never have a situation
    /// where more than f replicas fail at the same time, so they can always rely on the existence of other
    /// replicas that it can use to rebuild it's state from where it left off.
    ///
    /// One might say this provides no security benefits comparatively to storing information just in RAM (since
    /// we don't have any guarantees on what was actually stored in persistent storage)
    /// however this does provide more performance benefits as we don't have to rebuild the entire state from the
    /// other replicas of the system, which would degrade performance. We can take our incomplete state and
    /// just fill in the blanks using the state transfer algorithm
    Optimistic,

    /// Perform no persistent logging to the database and rely only on the prospect that
    /// We are always able to rebuild our state from other replicas that may be online
    None,
}

pub trait PersistentLogModeTrait: Send {
    fn init_persistent_log<D>(executor: ExecutorHandle<D>) -> PersistentLogMode<D>
        where
            D: ApplicationData + 'static;
}

///Strict log mode initializer
pub struct StrictPersistentLog;

impl PersistentLogModeTrait for StrictPersistentLog {
    fn init_persistent_log<D>(executor: ExecutorHandle<D>) -> PersistentLogMode<D>
        where
            D: ApplicationData + 'static,
    {
        let handle = ConsensusBacklog::init_backlog(executor);

        PersistentLogMode::Strict(handle)
    }
}

///Optimistic log mode initializer
pub struct OptimisticPersistentLog;

impl PersistentLogModeTrait for OptimisticPersistentLog {
    fn init_persistent_log<D: ApplicationData + 'static>(_: ExecutorHandle<D>) -> PersistentLogMode<D> {
        PersistentLogMode::Optimistic
    }
}

pub struct NoPersistentLog;

impl PersistentLogModeTrait for NoPersistentLog {
    fn init_persistent_log<D>(_: ExecutorHandle<D>) -> PersistentLogMode<D> where D: ApplicationData + 'static {
        PersistentLogMode::None
    }
}

///TODO: Handle sequence numbers that loop the u32 range.
/// This is the main reference to the persistent log, used to push data to it
pub struct PersistentLog<D: ApplicationData,
    OPM: OrderingProtocolMessage,
    SOPM: StatefulOrderProtocolMessage,
    STM: StateTransferMessage>
{
    persistency_mode: PersistentLogMode<D>,

    // A handle for the persistent log workers (each with his own thread)
    worker_handle: Arc<PersistentLogWorkerHandle<OPM, SOPM>>,

    p: PhantomData<STM>,
    ///The persistent KV-DB to be used
    db: KVDB,
}

/// The type of the installed state information
pub type InstallState<OPM: OrderingProtocolMessage, SOPM: StatefulOrderProtocolMessage> = (
    //The view sequence number
    View<OPM>,
    //The decision log that comes after that state
    DecLog<SOPM>,
);

/// Work messages for the persistent log workers
pub enum PWMessage<OPM: OrderingProtocolMessage, SOPM: StatefulOrderProtocolMessage> {
    //Persist a new view into the persistent storage
    View(View<OPM>),

    //Persist a new sequence number as the consensus instance has been committed and is therefore ready to be persisted
    Committed(SeqNo),

    // Persist the metadata for a given decision
    ProofMetadata(SerProofMetadata<OPM>),

    //Persist a given message into storage
    Message(Arc<ReadOnly<StoredMessage<ProtocolMessage<OPM>>>>),

    //Remove all associated stored messages for this given seq number
    Invalidate(SeqNo),

    // Register a proof of the decision log
    Proof(SerProof<OPM>),

    //Install a recovery state received from CST or produced by us
    InstallState(InstallState<OPM, SOPM>),

    /// Register a new receiver for messages sent by the persistency workers
    RegisterCallbackReceiver(ChannelSyncTx<ResponseMessage>),
}

/// The message containing the information necessary to persist the most recently received
/// Monolithic state
pub struct MonolithicStateMessage<S: MonolithicState> {
    checkpoint: Arc<ReadOnly<Checkpoint<S>>>,
}

/// The message containing the information necessary to persist the most recently received
/// State parts
pub enum DivisibleStateMessage<S: DivisibleState> {
    Parts(Vec<S::StatePart>),
    Descriptor(S::StateDescriptor),
    PartsAndDescriptor(Vec<S::StatePart>, S::StateDescriptor),
}

/// Messages sent by the persistency workers to notify the registered receivers
#[derive(Clone)]
pub enum ResponseMessage {
    ///Notify that we have persisted the view with the given sequence number
    ViewPersisted(SeqNo),

    ///Notifies that we have persisted the sequence number that has been persisted (Only the actual sequence number)
    /// Not related to actually persisting messages
    CommittedPersisted(SeqNo),

    // Notifies that the metadata for a given seq no has been persisted
    WroteMetadata(SeqNo),

    ///Notifies that a message with a given SeqNo and a given unique identifier for the message
    /// TODO: Decide this unique identifier
    WroteMessage(SeqNo, Digest),

    // Notifies that the state has been successfully installed and returns
    InstalledState(SeqNo),

    /// Notifies that all messages relating to the given sequence number have been destroyed
    InvalidationPersisted(SeqNo),

    /// Notifies that the given checkpoint was persisted into the database
    Checkpointed(SeqNo),
    /*
    WroteParts(Vec<Digest>),

    WroteDescriptor(SeqNo),

    WrotePartsAndDescriptor(SeqNo, Vec<Digest>),*/

    // Stored the proof with the given sequence
    Proof(SeqNo),

    RegisteredCallback,
}

/// Messages that are sent to the logging thread to log specific requests
pub(crate) type ChannelMsg<OPM: OrderingProtocolMessage, SOPM: StatefulOrderProtocolMessage> = (PWMessage<OPM, SOPM>, Option<CallbackType>);

pub fn initialize_persistent_log<D, K, T, OPM, SOPM, STM, POP, PSP>(executor: ExecutorHandle<D>, db_path: K)
                                                                    -> Result<PersistentLog<D, OPM, SOPM, STM>>
    where D: ApplicationData + 'static, K: AsRef<Path>, T: PersistentLogModeTrait,
          OPM: OrderingProtocolMessage + 'static,
          SOPM: StatefulOrderProtocolMessage + 'static,
          STM: StateTransferMessage + 'static,
          POP: PersistableOrderProtocol<OPM, SOPM> + Send + 'static,
          PSP: PersistableStateTransferProtocol + Send + 'static
{
    PersistentLog::init_log::<K, T, POP, PSP>(executor, db_path)
}

impl<D, OPM, SOPM, STM> PersistentLog<D, OPM, SOPM, STM>
    where D: ApplicationData + 'static,
          OPM: OrderingProtocolMessage + 'static,
          SOPM: StatefulOrderProtocolMessage + 'static,
          STM: StateTransferMessage + 'static,
          SOPM: StatefulOrderProtocolMessage + 'static,
{
    fn init_log<K, T, POS, PSP>(executor: ExecutorHandle<D>, db_path: K) -> Result<Self>
        where
            K: AsRef<Path>,
            T: PersistentLogModeTrait,
            POS: PersistableOrderProtocol<OPM, SOPM> + Send + 'static,
            PSP: PersistableStateTransferProtocol + Send + 'static
    {
        let mut message_types = POS::message_types();

        let mut prefixes = vec![COLUMN_FAMILY_OTHER, COLUMN_FAMILY_PROOFS];

        prefixes.append(&mut message_types);

        let log_mode = T::init_persistent_log(executor);

        let mut response_txs = vec![];

        match &log_mode {
            PersistentLogMode::Strict(handle) => response_txs.push(handle.logger_tx().clone()),
            _ => {}
        }

        let kvdb = KVDB::new(db_path, prefixes)?;

        let (tx, rx) = channel::new_bounded_sync(1024);

        let worker = PersistentLogWorker::<D, OPM, SOPM, POS, PSP>::new(rx, response_txs, kvdb.clone());

        match &log_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                std::thread::Builder::new().name(format!("Persistent log Worker #1"))
                    .spawn(move || {
                        worker.work();
                    }).unwrap();
            }
            _ => {}
        }

        let persistent_log_write_stub = PersistentLogWriteStub { tx };

        let worker_handle = Arc::new(PersistentLogWorkerHandle::new(vec![persistent_log_write_stub]));

        Ok(Self {
            persistency_mode: log_mode,
            worker_handle,
            p: Default::default(),
            db: kvdb,
        })
    }

    pub fn kind(&self) -> &PersistentLogMode<D> {
        &self.persistency_mode
    }

    ///Attempt to queue a batch into waiting for persistent logging
    /// If the batch does not have to wait, it's returned to it can be instantly
    /// passed to the executor
    pub fn wait_for_batch_persistency_and_execute(&self, batch: ProtocolConsensusDecision<D::Request>) -> Result<Option<ProtocolConsensusDecision<D::Request>>> {
        match &self.persistency_mode {
            PersistentLogMode::Strict(consensus_backlog) => {
                consensus_backlog.queue_batch(batch)?;

                Ok(None)
            }
            PersistentLogMode::Optimistic | PersistentLogMode::None => {
                Ok(Some(batch))
            }
        }
    }

    ///Attempt to queue a batch that was received in the form of a completed proof
    /// into waiting for persistent logging, instead of receiving message by message (Received in
    /// a view change)
    /// If the batch does not have to wait, it's returned to it can be instantly
    /// passed to the executor
    pub fn wait_for_proof_persistency_and_execute(&self, batch: ProtocolConsensusDecision<D::Request>) -> Result<Option<ProtocolConsensusDecision<D::Request>>> {
        match &self.persistency_mode {
            PersistentLogMode::Strict(backlog) => {
                backlog.queue_batch_proof(batch)?;

                Ok(None)
            }
            _ => {
                Ok(Some(batch))
            }
        }
    }
}

impl<D, OPM, SOPM, STM> OrderingProtocolLog<OPM> for PersistentLog<D, OPM, SOPM, STM>
    where D: ApplicationData + 'static,
          OPM: OrderingProtocolMessage + 'static,
          SOPM: StatefulOrderProtocolMessage + 'static,
          STM: StateTransferMessage + 'static {
    fn write_committed_seq_no(&self, write_mode: WriteMode, seq: SeqNo) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    WriteMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_committed(seq, callback)
                    }
                    WriteMode::BlockingSync => write_latest_seq_no(&self.db, seq),
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }

    fn write_view_info(&self, write_mode: WriteMode, view_seq: View<OPM>) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    WriteMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_view_number(view_seq, callback)
                    }
                    WriteMode::BlockingSync => {
                        todo!()
                    }
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }

    fn write_message(&self, write_mode: WriteMode, msg: Arc<ReadOnly<StoredMessage<ProtocolMessage<OPM>>>>) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    WriteMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_message(msg, callback)
                    }
                    WriteMode::BlockingSync => {
                        todo!()
                    }
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }

    fn write_proof_metadata(&self, write_mode: WriteMode, metadata: SerProofMetadata<OPM>) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    WriteMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_proof_metadata(metadata, callback)
                    }
                    WriteMode::BlockingSync => {
                        todo!()
                    }
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }

    fn write_proof(&self, write_mode: WriteMode, proof: SerProof<OPM>) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    WriteMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_proof(proof, callback)
                    }
                    WriteMode::BlockingSync => {
                        todo!()
                    }
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }

    fn write_invalidate(&self, write_mode: WriteMode, seq: SeqNo) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    WriteMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_invalidate(seq, callback)
                    }
                    WriteMode::BlockingSync => {
                        todo!()
                    }
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }
}

impl<D, OPM, SOPM, STM> StatefulOrderingProtocolLog<OPM, SOPM> for PersistentLog<D, OPM, SOPM, STM>
    where D: ApplicationData + 'static,
          OPM: OrderingProtocolMessage + 'static,
          SOPM: StatefulOrderProtocolMessage + 'static,
          STM: StateTransferMessage + 'static {
    fn read_state(&self, write_mode: WriteMode) -> Result<Option<(View<OPM>, DecLog<SOPM>)>> {
        match self.kind() {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                todo!();
                // let option = read_latest_state::<OPM, SOPM, PS>(&self.db)?;
                //
                // return if let Some((view, dec_log)) = option {
                //     Ok(Some((view, dec_log)))
                // } else {
                //     Ok(None)
                // };
            }
            PersistentLogMode::None => {
                Ok(None)
            }
        }
    }

    fn write_install_state(&self, write_mode: WriteMode, view: View<OPM>, dec_log: DecLog<SOPM>) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    WriteMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_install_state((view, dec_log), callback)
                    }
                    WriteMode::BlockingSync => {
                        todo!()
                    }
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }
}

impl<S, D, OPM, SOPM, STM> MonolithicStateLog<S> for PersistentLog<D, OPM, SOPM, STM>
    where S: MonolithicState,
          D: ApplicationData + 'static,
          OPM: OrderingProtocolMessage + 'static,
          SOPM: StatefulOrderProtocolMessage + 'static,
          STM: StateTransferMessage + 'static {
    fn write_checkpoint(&self, write_mode: WriteMode, checkpoint: Arc<ReadOnly<Checkpoint<S>>>) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    WriteMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_checkpoint(checkpoint, callback)
                    }
                    WriteMode::BlockingSync => {
                        todo!()
                    }
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
        todo!()
    }
}

impl<D: ApplicationData, OPM: OrderingProtocolMessage, SOPM: StatefulOrderProtocolMessage, STM: StateTransferMessage> Clone for PersistentLog<D, OPM, SOPM, STM> {
    fn clone(&self) -> Self {
        Self {
            persistency_mode: self.persistency_mode.clone(),
            worker_handle: self.worker_handle.clone(),
            p: Default::default(),
            db: self.db.clone(),
        }
    }
}

impl<D: ApplicationData> Clone for PersistentLogMode<D> {
    fn clone(&self) -> Self {
        match self {
            PersistentLogMode::Strict(handle) => {
                PersistentLogMode::Strict(handle.clone())
            }
            PersistentLogMode::Optimistic => {
                PersistentLogMode::Optimistic
            }
            PersistentLogMode::None => {
                PersistentLogMode::None
            }
        }
    }
}


impl<S, D, OPM, SOPM, STM> DivisibleStateLog<S> for PersistentLog<D, OPM, SOPM, STM>
    where S: DivisibleStateState,
          D: ApplicationData + 'static,
          OPM: OrderingProtocolMessage + 'static,
          SOPM: StatefulOrderProtocolMessage + 'static,
          STM: StateTransferMessage + 'static {

          }

