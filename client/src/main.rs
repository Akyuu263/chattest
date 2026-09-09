use std::io::{self, ErrorKind, Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::{self, TryRecvError};
use std::thread;
use std::time::Duration;
use std::env::args;

const MAX_MSG_SIZE: usize = 1024;
const MSG_TYPE_CHAT: u8 = 1;
const LENGTH_SIZE:u32 = 4;

fn main() {
    let ip = args().nth(1).unwrap_or_else(|| "127.0.0.1".to_string());
    let addr = format!("{}:6000", ip);
    let mut client = TcpStream::connect(addr).expect("Stream failed to connect");
    client.set_nonblocking(true).expect("failed to initiate non-blocking");

    let (tx, rx) = mpsc::channel::<String>();

    thread::spawn(move || { 
        let mut buff = vec![];
        loop {
            let mut temp = [0; MAX_MSG_SIZE + 1 + LENGTH_SIZE as usize];
            match client.read(&mut temp) {
                Ok(0) => {
                    println!("closing connection with server");
                    break;
                },
                Ok(n) => {
                    buff.extend_from_slice(&temp[..n]);
                },
                Err(ref err) if err.kind() == ErrorKind::WouldBlock => (),
                Err(_) => {
                    println!("connect with server was severed");
                    break;
                }
            }

            while buff.len() > LENGTH_SIZE as usize {
                let length = u32::from_le_bytes(buff[..4].try_into().unwrap());
                if length as usize > MAX_MSG_SIZE || buff.len() < (LENGTH_SIZE + length) as usize {
                    break;
                }
                buff.drain(..4);
                let mut msg = buff.drain(..length as usize).collect::<Vec<_>>();
                let msg_type = msg.remove(0);
                if msg_type == MSG_TYPE_CHAT {
                    let msg = String::from_utf8_lossy(&msg).into_owned();
                    println!("message recv {:?}", msg);
                }
            }

            match rx.try_recv() {
                Ok(msg) => {
                    let len = 1 + msg.len() as u32;
                    let mut buff = len.to_le_bytes().to_vec();
                    buff.push(MSG_TYPE_CHAT);
                    buff.extend_from_slice(msg.as_bytes());
                    if let Ok(()) = client.write_all(&buff) {
                        println!("message sent {:?}", msg);
                    } else {
                        println!("writing to socket failed");
                    }
                }
                Err(TryRecvError::Empty) => (),
                Err(TryRecvError::Disconnected) => break
            }

            thread::sleep(Duration::from_millis(100));
        }
    });

    println!("Write a Message:");
    loop {
        let mut buff = String::new();
        io::stdin().read_line(&mut buff).expect("reading from stdin failed");
        let msg = buff.trim().to_string();
        if msg.len() + 1 > MAX_MSG_SIZE {
            println!("sending message failed, context too long");
            continue;
        }
        if msg == ":quit" || tx.send(msg).is_err() {break}
    }
    println!("bye bye");
}
