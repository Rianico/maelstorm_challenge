use std::io::Write;

use anyhow::Context;
use rustgen::main_loop;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum EchoMessage {
    Echo { echo: String },
    EchoOk { echo: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EchoNode {
    msg_id: usize,
}

impl rustgen::Node<EchoMessage> for EchoNode {
    fn init_from(_: &rustgen::InitBody) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self { msg_id: 1 })
    }

    fn step(
        &mut self,
        req: rustgen::Message<EchoMessage>,
        output: &mut impl Write,
    ) -> anyhow::Result<()> {
        let mut msg = req.into_reply(Some(&mut self.msg_id));
        match msg.body.payload {
            EchoMessage::Echo { echo } => {
                msg.body.payload = EchoMessage::EchoOk { echo };
                msg.serialize(&mut serde_json::Serializer::new(&mut *output))
                    .context("serialize echo_ok message failed")?;
                output.write_all(b"\n")?;
            }
            EchoMessage::EchoOk { .. } => unreachable!(),
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<EchoMessage, EchoNode>()?;
    Ok(())
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use rustgen::{Body, Message};
    use serde::Serialize;

    use crate::EchoMessage;

    #[test]
    fn test_echo_node_msg() -> anyhow::Result<()> {
        let echo_ok_msg = EchoMessage::EchoOk {
            echo: "echo".to_string(),
        };
        let msg = Message {
            src: "c1".to_string(),
            dst: "n1".to_string(),
            body: Body {
                payload: echo_ok_msg,
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

    #[test]
    fn test_stdout() -> anyhow::Result<()> {
        let mut out = std::io::stdout().lock();
        out.write(b"hello")?;
        out.flush()?;
        out.write(b"zxk")?;
        // out.write_all(b"hello2")?;
        Ok(())
    }
}
