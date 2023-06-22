use std::{
    collections::{HashMap, HashSet},
    io::Write,
    time::Duration,
};

use anyhow::Context;
use rand::Rng;
use rustgen::{main_loop, Body, Message};
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
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,

    Extended(GossipProtocol),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipProtocol {
    GossipAlert,
    Gossip { messages: HashSet<usize> },
}

struct BroadcastNode {
    id: String,
    msg_id: usize,
    messages: HashSet<usize>,
    neightbors: Vec<String>,
    known: HashMap<String, HashSet<usize>>,
}

impl BroadcastNode {
    fn handle_external(
        &mut self,
        req: &rustgen::Message<BroadcastMessage>,
        output: &mut impl Write,
        external: &GossipProtocol,
    ) -> anyhow::Result<()> {
        match external {
            GossipProtocol::GossipAlert => {
                let mut rnd = rand::thread_rng();
                // todo use parallel stream to speed up
                for neighbor in &self.neightbors {
                    let known_msg = &self.known[neighbor];
                    let (known, mut unknown): (HashSet<usize>, HashSet<usize>) = self
                        .messages
                        .iter()
                        .partition(|msg| known_msg.contains(msg));
                    let additional_cap = unknown.len().min(3236 * known.len() / 10000) as u32;
                    unknown.extend(
                        known
                            .iter()
                            .filter(|_| rnd.gen_ratio(additional_cap, known.len() as u32)),
                    );
                    Message {
                        src: self.id.clone(),
                        dst: neighbor.clone(),
                        body: Body {
                            id: Default::default(),
                            in_reply_to: Default::default(),
                            payload: BroadcastMessage::Extended(GossipProtocol::Gossip {
                                messages: unknown,
                            }),
                        },
                    }
                    .send(output)
                    .with_context(|| format!("send gossip to {}", neighbor))?
                }
                Ok(())
            }
            GossipProtocol::Gossip { messages } => {
                self.known
                    .get_mut(&req.src)
                    .with_context(|| format!("can't find the neighbor {}", req.src))
                    .expect("update known message failed")
                    .extend(messages);
                self.messages.extend(messages.iter().copied());
                Ok(())
            }
        }
    }
}

impl rustgen::Node<BroadcastMessage> for BroadcastNode {
    fn init_from(
        init_msg: &rustgen::InitBody,
        tx: std::sync::mpsc::Sender<Message<BroadcastMessage>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        // create a thread to send gossip notification in period
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(100));
            let _ = tx.send(Message {
                src: Default::default(),
                dst: Default::default(),
                body: Body {
                    id: None,
                    in_reply_to: None,
                    payload: BroadcastMessage::Extended(GossipProtocol::GossipAlert),
                },
            });
        });
        let neightbors = init_msg.node_ids.clone();
        Ok(Self {
            id: init_msg.node_id.clone(),
            msg_id: 1,
            messages: HashSet::new(),
            known: neightbors
                .iter()
                .map(|node_id| (node_id.clone(), HashSet::default()))
                .collect::<HashMap<String, HashSet<usize>>>(),
            neightbors,
        })
    }

    fn step(
        &mut self,
        mut req: rustgen::Message<BroadcastMessage>,
        output: &mut impl Write,
    ) -> anyhow::Result<()> {
        match req.body.payload {
            BroadcastMessage::Broadcast { message } => {
                self.messages.insert(message);
                let mut reply = req.into_reply(Some(&mut self.msg_id));
                reply.body.payload = BroadcastMessage::BroadcastOk;
                reply.send(output)?
            }
            BroadcastMessage::Read => {
                let mut reply = req.into_reply(Some(&mut self.msg_id));
                let mut tmp_messages = HashSet::with_capacity(0);
                std::mem::swap(&mut self.messages, &mut tmp_messages);
                reply.body.payload = BroadcastMessage::ReadOk {
                    messages: tmp_messages,
                };
                reply.send(output)?;
                let BroadcastMessage::ReadOk { mut messages } = reply.body.payload else {
                    unreachable!()
                };
                std::mem::swap(&mut self.messages, &mut messages);
            }
            BroadcastMessage::Topology { ref mut topology } => {
                self.neightbors = topology
                    .remove(&self.id)
                    .unwrap_or_else(|| panic!("no topology given for node {}", self.id));
                let mut reply = req.into_reply(Some(&mut self.msg_id));
                reply.body.payload = BroadcastMessage::TopologyOk;
                reply.send(output)?
            }
            BroadcastMessage::TopologyOk
            | BroadcastMessage::BroadcastOk
            | BroadcastMessage::ReadOk { .. } => {}
            BroadcastMessage::Extended(ref external) => {
                self.handle_external(&req, output, &external)?
            }
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<BroadcastMessage, BroadcastNode>()?;
    Ok(())
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use rustgen::{Body, Message};
    use serde::Serialize;

    use crate::{BroadcastMessage, GossipProtocol};

    #[test]
    fn test_echo_node_msg() -> anyhow::Result<()> {
        let ext = BroadcastMessage::Extended(GossipProtocol::Gossip {
            messages: HashSet::default(),
        });
        let msg = Message {
            src: "c1".to_string(),
            dst: "n1".to_string(),
            body: Body {
                payload: ext,
                id: None,
                in_reply_to: None,
            },
        }
        .into_reply(Some(&mut 1));
        let stdout = std::io::stdout().lock();
        let mut output = serde_json::Serializer::new(stdout);
        msg.serialize(&mut output)?;
        Ok(())
    }
}
