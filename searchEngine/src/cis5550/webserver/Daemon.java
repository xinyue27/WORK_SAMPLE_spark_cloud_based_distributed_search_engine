package cis5550.webserver;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

public class Daemon extends Thread{
	private ServerSocket serverSock;
	private BlockingQueue<Socket> blockingQueue;
	
	public Daemon(ServerSocket serverSock, BlockingQueue<Socket> blockingQueue) {
		this.serverSock = serverSock;
		this.blockingQueue = blockingQueue;
	}
	
	/*
	 * A daemon accepts new connection and puts it in a BlockingQueue.
	 */
	public void run() {
		while (true) {
			try {
				Socket sock = serverSock.accept();
				blockingQueue.put(sock);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("ERR: unable to accept new client socket");
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				System.out.println("ERR: unable to add new sock to blockingQueue");
				e.printStackTrace();
			}
		}
	}

}
