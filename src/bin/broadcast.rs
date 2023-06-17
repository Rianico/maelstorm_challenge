use std::{
    collections::{HashMap, HashSet},
    io::Write,
};

use anyhow::Context;
use rustgen::main_loop;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum BroadcastMessage {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<usize>>,
    },
    TopologyOk,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BroadcastNode {
    id: String,
    msg_id: usize,
    messages: HashSet<usize>,
}

impl rustgen::Node<BroadcastMessage> for BroadcastNode {
    fn init_from(init_msg: &rustgen::InitBody) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            id: init_msg.node_id.clone(),
            msg_id: 1,
            messages: HashSet::new(),
        })
    }

    fn step(
        &mut self,
        req: rustgen::Message<BroadcastMessage>,
        output: &mut impl Write,
    ) -> anyhow::Result<()> {
        match req.body.payload {
            BroadcastMessage::Broadcast { message } => {
                self.messages.insert(message);
                let mut reply = req.into_reply(Some(&mut self.msg_id));
                reply.body.payload = BroadcastMessage::BroadcastOk;
                serde_json::to_writer(&mut *output, &reply)
                    .context("serde to broadcast_ok message filed")?;
                output.write_all(b"\n")?;
            }
            BroadcastMessage::Read => {
                let mut reply = req.into_reply(Some(&mut self.msg_id));
                reply.body.payload = BroadcastMessage::ReadOk {
                    messages: self.messages.clone(),
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serde to broadcast_ok message filed")?;
                output.write_all(b"\n")?;
            }
            BroadcastMessage::Topology { .. } => {
                let mut reply = req.into_reply(Some(&mut self.msg_id));
                reply.body.payload = BroadcastMessage::TopologyOk;
                serde_json::to_writer(&mut *output, &reply)
                    .context("serde to broadcast_ok message filed")?;
                output.write_all(b"\n")?;
            }
            BroadcastMessage::TopologyOk
            | BroadcastMessage::BroadcastOk
            | BroadcastMessage::ReadOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<BroadcastMessage, BroadcastNode>()?;
    Ok(())
}
