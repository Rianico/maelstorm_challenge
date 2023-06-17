use std::io::Write;

use anyhow::Context;
use rustgen::{main_loop, Message};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum Generation {
    Generate,
    GenerateOk {
        #[serde(rename = "id")]
        unique_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UniqueNode {
    id: String,
    msg_id: usize,
}

impl rustgen::Node<Generation> for UniqueNode {
    fn init_from(
        init_msg: &rustgen::InitBody,
        _: std::sync::mpsc::Sender<Message<Generation>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            id: init_msg.node_id.clone(),
            msg_id: 1,
        })
    }

    fn step(
        &mut self,
        req: rustgen::Message<Generation>,
        output: &mut impl Write,
    ) -> anyhow::Result<()> {
        let mut msg = req.into_reply(Some(&mut self.msg_id));
        match msg.body.payload {
            Generation::Generate => {
                msg.body.payload = Generation::GenerateOk {
                    unique_id: format!("{}-{}", self.id, self.msg_id),
                };
                msg.serialize(&mut serde_json::Serializer::new(&mut *output))
                    .context("serialize echo_ok message failed")?;
                output.write_all(b"\n")?;
            }
            Generation::GenerateOk { .. } => unreachable!(),
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<Generation, UniqueNode>()?;
    Ok(())
}
