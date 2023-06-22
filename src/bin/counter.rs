use std::{collections::HashMap, io::Write, time::Duration};

use anyhow::Context;

use rustgen::{main_loop, Body, Message};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum GlobalCounter {
    Add { delta: usize },
    AddOk,
    Read,
    ReadOk { value: usize },
    Extended(GossipProtocol),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum GossipProtocol {
    GossipAlert,
    Gossip { counter: Counter },
}

struct BroadcastNode {
    id: String,
    msg_id: usize,
    neightbors: Vec<String>,
    inner: Counter,
}

impl BroadcastNode {
    fn counter(&self) -> &Counter {
        &self.inner
    }

    fn counter_mut(&mut self) -> &mut Counter {
        &mut self.inner
    }

    fn send_to_neighbor(&self, neighbor: &str, output: &mut impl Write) -> anyhow::Result<()> {
        Message {
            src: self.id.clone(),
            dst: neighbor.to_string(),
            body: Body {
                id: Default::default(),
                in_reply_to: Default::default(),
                payload: GlobalCounter::Extended(GossipProtocol::Gossip {
                    counter: self.counter().clone(),
                }),
            },
        }
        .send(output)
        .with_context(|| format!("send gossip to {}", neighbor))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Counter {
    counter: HashMap<String, usize>,
}

impl Counter {
    fn add(&mut self, key: String, delta: usize) {
        self.counter
            .entry(key)
            .and_modify(|v| *v += delta)
            .or_insert(delta);
    }

    fn sum(&self) -> usize {
        self.counter.values().sum()
    }

    fn merge(&mut self, counter: Counter) {
        counter.counter.into_iter().for_each(|(k, v)| {
            self.counter
                .entry(k)
                .and_modify(|value| *value = v.max(*value))
                .or_insert(v);
        });
    }
}

impl rustgen::Node<GlobalCounter> for BroadcastNode {
    fn init_from(
        init_msg: &rustgen::InitBody,
        tx: std::sync::mpsc::Sender<Message<GlobalCounter>>,
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
                    payload: GlobalCounter::Extended(GossipProtocol::GossipAlert),
                },
            });
        });
        let neightbors = init_msg.node_ids.clone();
        let counter = Counter {
            counter: neightbors
                .iter()
                .map(|node_id| (node_id.clone(), usize::default()))
                .collect::<HashMap<String, usize>>(),
        };
        Ok(Self {
            id: init_msg.node_id.clone(),
            msg_id: 1,
            neightbors,
            inner: counter,
        })
    }

    fn step(
        &mut self,
        req: rustgen::Message<GlobalCounter>,
        output: &mut impl Write,
    ) -> anyhow::Result<()> {
        match req.body.payload {
            GlobalCounter::Add { delta } => {
                self.inner.add(req.dst.clone(), delta);
                let mut reply = req.into_reply(Some(&mut self.msg_id));
                reply.body.payload = GlobalCounter::AddOk;
                reply.send(output)?
            }
            GlobalCounter::Read => {
                let mut reply = req.into_reply(Some(&mut self.msg_id));
                reply.body.payload = GlobalCounter::ReadOk {
                    value: self.counter().sum(),
                };
                reply.send(output)?;
            }
            GlobalCounter::Extended(GossipProtocol::GossipAlert) => {
                for neighbor in self.neightbors.iter().filter(|node| **node != self.id) {
                    self.send_to_neighbor(neighbor.as_str(), output)?
                }
            }
            GlobalCounter::Extended(GossipProtocol::Gossip { counter }) => {
                self.counter_mut().merge(counter)
            }
            GlobalCounter::ReadOk { .. } | GlobalCounter::AddOk => unreachable!(),
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<GlobalCounter, BroadcastNode>()?;
    Ok(())
}
