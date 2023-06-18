use std::{
    collections::{HashMap, HashSet},
    io::Write,
    time::Duration,
};

use anyhow::Context;
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

    External(ExternalMessage),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExternalMessage {
    GossipInform,
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
        external: &ExternalMessage,
    ) -> anyhow::Result<()> {
        match external {
            ExternalMessage::GossipInform => {
                for neighbor in &self.neightbors {
                    let known_msg = self
                        .known
                        .get(neighbor)
                        .with_context(|| format!("can't find the neighbor {}", neighbor))
                        .expect("get known message failed");
                    let messages: HashSet<usize> = self
                        .messages
                        .iter()
                        .filter(|msg| !known_msg.contains(msg))
                        .copied()
                        .collect();
                    eprintln!("{} / {}", messages.len(), self.messages.len());
                    Message {
                        src: self.id.clone(),
                        dst: neighbor.clone(),
                        body: Body {
                            id: Default::default(),
                            in_reply_to: Default::default(),
                            payload: BroadcastMessage::External(ExternalMessage::Gossip {
                                messages,
                            }),
                        },
                    }
                    .send(&mut *output)
                    .with_context(|| format!("send gossip to {}", neighbor))?
                }
                Ok(())
            }
            ExternalMessage::Gossip { messages } => {
                self.known
                    .get_mut(&req.src)
                    .with_context(|| format!("can't find the neighbor {}", req.src))
                    .expect("update known message failed")
                    .extend(messages);
                self.messages.extend(messages);
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
            std::thread::sleep(Duration::from_millis(200));
            let _ = tx.send(Message {
                src: Default::default(),
                dst: Default::default(),
                body: Body {
                    id: None,
                    in_reply_to: None,
                    payload: BroadcastMessage::External(ExternalMessage::GossipInform),
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
                reply.body.payload = BroadcastMessage::ReadOk {
                    messages: self.messages.clone(),
                };
                reply.send(output)?
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
            BroadcastMessage::External(ref external) => {
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

    use crate::{BroadcastMessage, ExternalMessage};

    #[test]
    fn test_echo_node_msg() -> anyhow::Result<()> {
        let ext = BroadcastMessage::External(ExternalMessage::Gossip {
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
