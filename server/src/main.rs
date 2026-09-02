use std::io::{ErrorKind, Read, Write};
use std::collections::VecDeque;
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;

const LOCAL: &str = "0.0.0.0:6000";
const MSG_SIZE: usize = 32;

fn sleep() {
    thread::sleep(::std::time::Duration::from_millis(100));
}

fn main() {
    let server = TcpListener::bind(LOCAL).expect("Listener failed to bind");
    server.set_nonblocking(true).expect("failed to initialize non-blocking");

    let mut clients: Vec<(TcpStream, VecDeque<Vec<u8>>)> = vec![];
    let (tx, rx) = mpsc::channel::<String>();
    loop {
        if let Ok((mut socket, addr)) = server.accept() {
            println!("Client {} connected", addr);
            let tx = tx.clone();
            clients.push((socket.try_clone().expect("failed to clone client"), VecDeque::new()));

            thread::spawn(move || {
                let mut buff = vec![];
                loop {
                    let mut temp = [0; MSG_SIZE * 2];
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

                    while buff.len() >= MSG_SIZE {
                        let msg = buff.drain(..MSG_SIZE).take_while(|&x| x !=0 ).collect::<Vec<_>>();
                        let msg = String::from_utf8_lossy(&msg).into_owned();
                        println!("{}: {:?}", addr, msg);
                        tx.send(msg).expect("failed to send msg to rx");
                    }
                
                    sleep();
                }
            });
        }

        if let Ok(msg) = rx.try_recv() {
            let mut buff = msg.clone().into_bytes();
            buff.resize(MSG_SIZE,0);
            clients.iter_mut().for_each(|client| client.1.push_back(buff.clone()));
        }

        clients.retain_mut(|client| {
            while let Some(buff) = client.1.pop_front() {
                match client.0.write_all(&buff) {
                    Ok(_) => (),
                    Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                        client.1.push_front(buff);
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
