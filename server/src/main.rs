use std::io::{ErrorKind, Read, Write};
use std::collections::VecDeque;
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::sync::mpsc;
use std::thread;

const LOCAL: &str = "0.0.0.0:6000";
const MAX_MSG_SIZE: usize = 1024;
const MAX_NAME_LONG: usize = 16;
const MSG_TYPE_CHAT: u8 = 1;
const MSG_TYPE_JOIN: u8 = 2;
const LENGTH_SIZE:u32 = 4;

struct Upstream {
    addr: SocketAddr,
    msg_type: u8,
    msg: String,
}

struct Client {
    addr: SocketAddr,
    name: String,
    stream: TcpStream,
    msg_queue: VecDeque<Vec<u8>>,
}

fn sleep() {
    thread::sleep(::std::time::Duration::from_millis(100));
}

fn main() {
    let server = TcpListener::bind(LOCAL).expect("Listener failed to bind");
    server.set_nonblocking(true).expect("failed to initialize non-blocking");

    let mut clients: Vec<Client> = vec![];
    let (tx, rx) = mpsc::channel::<Upstream>();
    loop {
        if let Ok((mut socket, addr)) = server.accept() {
            println!("Client {} connected", addr);
            let tx = tx.clone();
            let stream = socket.try_clone().expect("failed to clone client");
            let queue = VecDeque::new();
            let client = Client {
                addr,
                name: "".to_string(),
                stream,
                msg_queue: queue
            };
            clients.push(client);

            thread::spawn(move || {
                let mut buff = vec![];
                loop {
                    let mut temp = [0; MAX_MSG_SIZE + 1 + LENGTH_SIZE as usize];
                    match socket.read(&mut temp) {
                        Ok(0) => {
                            println!("closing connection with: {}", addr);
                            break;
                        },
                        Ok(n) => {
                            buff.extend_from_slice(&temp[..n]);
                        },
                        Err(ref err) if err.kind() == ErrorKind::WouldBlock => (),
                        Err(_) => {
                            println!("connection error with: {}", addr);
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
                        let msg = String::from_utf8_lossy(&msg).into_owned();
                        if msg_type == MSG_TYPE_CHAT {
                            println!("{}: {:?}", addr, msg);
                        }
                        let upstream = Upstream {
                            addr,
                            msg_type,
                            msg,
                        };
                        tx.send(upstream).expect("failed to send msg to rx");
                    }

                    sleep();
                }
            });
        }

        if let Ok(mut upstream) = rx.try_recv() {
            let msg = &mut upstream.msg;
            let len = 1 + msg.len() as u32;
            let mut buff = len.to_le_bytes().to_vec();
            match upstream.msg_type {
                MSG_TYPE_CHAT => {
                    buff.push(MSG_TYPE_CHAT);
                    buff.extend_from_slice(msg.as_bytes());
                    clients.iter_mut().for_each(|client| client.msg_queue.push_back(buff.clone()));
                }
                MSG_TYPE_JOIN => {
                    let msg = msg.chars().take(MAX_NAME_LONG).collect::<String>();
                    if let Some(client)  =clients.iter_mut().find(|client| client.addr == upstream.addr) {
                        client.name = msg;
                        println!("{} name = {:?}", client.addr, client.name);
                    }
                }
                _ => {
                    println!("ignore");
                }
            }
        }

        clients.retain_mut(|client| {
            while let Some(buff) = client.msg_queue.pop_front() {
                match client.stream.write_all(&buff) {
                    Ok(_) => (),
                    Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                        client.msg_queue.push_front(buff);
                        break;
                    },
                    Err(_) => {
                        return false;
                    }
                }
            }
            true
        });
        sleep();
    }
}
