use std::io::{self, ErrorKind, Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::{self, TryRecvError};
use std::thread;
use std::time::Duration;
use std::env::args;

const MSG_SIZE: usize = 32;

fn main() {
    let ip = args().nth(1).unwrap_or_else(|| "127.0.0.1".to_string());
    let addr = format!("{}:6000", ip);
    let mut client = TcpStream::connect(addr).expect("Stream failed to connect");
    client.set_nonblocking(true).expect("failed to initiate non-blocking");

    let (tx, rx) = mpsc::channel::<String>();

    thread::spawn(move || { 
        let mut buff = vec![];
        loop {
            let mut temp = [0; MSG_SIZE * 2];
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

            while buff.len() >= MSG_SIZE {
                let msg = buff.drain(..MSG_SIZE).take_while(|&x| x !=0 ).collect::<Vec<_>>();
                let msg = String::from_utf8_lossy(&msg).into_owned();
                println!("message recv {:?}", msg);
            }

            match rx.try_recv() {
                Ok(msg) => {
                    let mut buff = msg.clone().into_bytes();
                    buff.resize(MSG_SIZE, 0);
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
        if msg == ":quit" || tx.send(msg).is_err() {break}
    }
    println!("bye bye");
}
