//! User application execution business logic.

use std::sync::Arc;
use std::time::Instant;

use atlas_common::threadpool;
use atlas_common::error::*;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::FullNetworkNode;
use atlas_communication::protocol_node::ProtocolNetworkNode;
use atlas_core::messages::ReplyMessage;
use atlas_core::ordering_protocol::OrderingProtocol;
use atlas_core::smr::exec::{ReplyNode, ReplyType};
use atlas_execution::app::BatchReplies;
use atlas_execution::serialize::ApplicationData;
use atlas_metrics::metrics::metric_duration;

use crate::metric::REPLIES_SENT_TIME_ID;

pub mod monolithic_executor;
pub mod divisible_state_exec;

const EXECUTING_BUFFER: usize = 16384;
//const REPLY_CONCURRENCY: usize = 4;

pub trait ExecutorReplier: Send {
    fn execution_finished<D, NT>(
        node: Arc<NT>,
        seq: Option<SeqNo>,
        batch: BatchReplies<D::Reply>,
    ) where D: ApplicationData + 'static,
            NT: ReplyNode<D> + 'static;
}

pub struct FollowerReplier;

impl ExecutorReplier for FollowerReplier {
    fn execution_finished<D, NT>(
        node: Arc<NT>,
        seq: Option<SeqNo>,
        batch: BatchReplies<D::Reply>,
    ) where D: ApplicationData + 'static,
            NT: ReplyNode<D> + 'static {
        if let None = seq {
            //Followers only deliver replies to the unordered requests, since it's not part of the quorum
            // And the requests it executes are only forwarded to it

            ReplicaReplier::execution_finished::<D, NT>(node, seq, batch);
        }
    }
}

pub struct ReplicaReplier;

impl ExecutorReplier for ReplicaReplier {
    fn execution_finished<D, NT>(
        mut send_node: Arc<NT>,
        _seq: Option<SeqNo>,
        batch: BatchReplies<D::Reply>,
    ) where D: ApplicationData + 'static,
            NT: ReplyNode<D> + 'static {
        if batch.len() == 0 {
            //Ignore empty batches.
            return;
        }

        let start = Instant::now();

        threadpool::execute(move || {
            let mut batch = batch.into_inner();

            batch.sort_unstable_by_key(|update_reply| update_reply.to());

            // keep track of the last message and node id
            // we iterated over
            let mut curr_send = None;

            for update_reply in batch {
                let (peer_id, session_id, operation_id, payload) = update_reply.into_inner();

                // NOTE: the technique used here to peek the next reply is a
                // hack... when we port this fix over to the production
                // branch, perhaps we can come up with a better approach,
                // but for now this will do
                if let Some((message, last_peer_id)) = curr_send.take() {
                    let flush = peer_id != last_peer_id;
                    send_node.send(ReplyType::Ordered, message, last_peer_id, flush);
                }

                // store previous reply message and peer id,
                // for the next iteration
                //TODO: Choose ordered or unordered reply
                let message = ReplyMessage::new(session_id, operation_id, payload);

                curr_send = Some((message, peer_id));
            }

            // deliver last reply
            if let Some((message, last_peer_id)) = curr_send {
                send_node.send(ReplyType::Ordered, message, last_peer_id, true);
            } else {
                // slightly optimize code path;
                // the previous if branch will always execute
                // (there is always at least one request in the batch)
                unreachable!();
            }

            metric_duration(REPLIES_SENT_TIME_ID, start.elapsed());
        });
    }
}