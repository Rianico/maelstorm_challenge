use std::{
    fmt::Debug,
    io::{stdout, BufReader, Write},
};

use anyhow::Context;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<MessageType> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<MessageType>,
}

impl<M: Serialize> Message<M> {
    pub fn into_reply(self, msg_id: Option<&mut usize>) -> Self {
        Self {
            src: self.dst,
            dst: self.src,
            body: Body {
                payload: self.body.payload,
                id: msg_id.map(|id| {
                    let mid = *id;
                    *id += 1;
                    mid
                }),
                in_reply_to: self.body.id,
            },
        }
    }

    pub fn send(&self, output: &mut impl Write) -> anyhow::Result<()> {
        serde_json::to_writer(&mut *output, &self)
            .context("serde to broadcast_ok message filed")?;
        output.write_all(b"\n").context("flush message error")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<MessageType> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: MessageType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum InitMsg {
    Init(InitBody),
    InitOk,
}

impl Message<InitMsg> {
    pub fn into_init_ok(&self) -> anyhow::Result<Self> {
        match &self.body.payload {
            InitMsg::Init(..) => Ok(Message {
                src: self.dst.clone(),
                dst: self.src.clone(),
                body: Body {
                    id: None,
                    in_reply_to: self.body.id,
                    payload: InitMsg::InitOk,
                },
            }),
            InitMsg::InitOk => anyhow::bail!("can't convert from init_ok messag"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitBody {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

pub trait Node<MessageType> {
    fn init_from(
        init: &InitBody,
        tx: std::sync::mpsc::Sender<Message<MessageType>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn step(&mut self, req: Message<MessageType>, output: &mut impl Write) -> anyhow::Result<()>;
}

pub fn main_loop<MessageType, N>() -> anyhow::Result<()>
where
    MessageType: DeserializeOwned + Send + 'static,
    N: Node<MessageType> + Send + 'static,
{
    let mut stdin = BufReader::new(std::io::stdin().lock());
    let init_msg = serde_json::Deserializer::from_reader(&mut stdin)
        .into_iter::<Message<InitMsg>>()
        .next()
        .expect("no init msg received at first")
        .context("construct init message failed")?;
    let InitMsg::Init ( ref init_body ) = init_msg.body.payload else {
        panic!("first message should be init.");
    };

    {
        let mut stdout = stdout().lock();
        serde_json::to_writer(&mut stdout, &init_msg.into_init_ok()?)?;
        stdout.write_all(b"\n")?;
    }

    let (tx, rx) = std::sync::mpsc::channel();

    let mut node: N = Node::init_from(init_body, tx.clone())
        .context("construct node from init message failed")
        .expect("Fail to construct the node from init msg");

    let jh = std::thread::spawn(move || {
        let mut stdout = stdout().lock();
        for msg in rx {
            node.step(msg, &mut stdout).expect("step msg error");
        }
    });

    let stdin =
        serde_json::Deserializer::from_reader(&mut stdin).into_iter::<Message<MessageType>>();
    for line in stdin {
        let msg = line.context("Maelstrom input from STDIN could not be read")?;
        if let Err(_) = tx.send(msg) {
            return Ok::<_, anyhow::Error>(());
        }
    }

    jh.join().expect("stdout thread error");
    Ok(())
}

#[cfg(test)]
mod test {
    use serde::Serialize;

    use crate::{Body, InitBody, InitMsg, Message};

    #[test]
    fn name() -> anyhow::Result<()> {
        let init = InitMsg::Init(InitBody {
            node_id: "n1".to_string(),
            node_ids: vec!["n1".to_string(), "n2".to_string()],
        });
        let msg = Message {
            src: "c1".to_string(),
            dst: "n1".to_string(),
            body: Body {
                payload: init,
                id: Some(1),
                in_reply_to: Some(1),
            },
        };
        let stdout = std::io::stdout().lock();
        let mut output = serde_json::Serializer::new(stdout);
        msg.serialize(&mut output)?;
        Ok(())
    }

    #[test]
    fn serde_from_str() -> anyhow::Result<()> {
        let content = r#"{"src":"c1","dest":"n1","body":{"type":"init","node_id":"n1","node_ids":["n1","n2"],"msg_id":1,"in_reply_to":1}}"#;
        let msg: Message<InitMsg> = serde_json::from_str(content)?;
        println!("{msg:?}");
        Ok(())
    }
}
