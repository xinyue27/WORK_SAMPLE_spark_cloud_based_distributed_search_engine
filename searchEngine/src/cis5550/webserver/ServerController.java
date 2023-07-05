package cis5550.webserver;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import javax.net.ServerSocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

public class ServerController {
	private String dir;
	private int httpPort;
	private int httpsPort;
	private ServerSocket serverSock;

	private BlockingQueue<Socket> blockingQueue;
	private List<ServerWorker> workersPool;
	private final int NUM_WORKERS = 10;

	public ServerController(int httpPort, int httpsPort, String dir) {
		this.dir = dir;
		this.httpPort = httpPort;
		this.httpsPort = httpsPort;
		this.blockingQueue = new LinkedBlockingDeque<Socket>();
		this.workersPool = new ArrayList<ServerWorker>();
	}

	/*
	 * EC1. 
	 * Start a daemon that accepts new connection and puts it in a BlockingQueue.
	 * Start NUM_WORKERS=10 worker threads that pull connections out of BlockingQueue and handle them.
	 */
//	public void start() {
//		try {
//			serverSock = new ServerSocket(this.port);
//			for (int i = 0; i < NUM_WORKERS; i++) {
//				ServerWorker newWorker = new ServerWorker(this.blockingQueue, this.dir);
//				this.workersPool.add(newWorker);
//				newWorker.start();
//			}
//			Daemon daemon = new Daemon(serverSock, this.blockingQueue);
//			daemon.start();
//
//		} catch (IOException e) {
//			System.out.println("ERR: socket connection is closed.");
//			e.printStackTrace();
//		}
//	}
	
	public void start() {
		String pwd = "secret";
		KeyStore keyStore;
		try {
			serverSock = new ServerSocket(this.httpPort);
//			System.out.println("this.httpsPort: " + this.httpsPort + " this.httpPort: " + this.httpPort);
			if (this.httpsPort != -1) {
				keyStore = KeyStore.getInstance("JKS");
				keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
				KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
				keyManagerFactory.init(keyStore, pwd.toCharArray());
				SSLContext sslContext = SSLContext.getInstance("TLS");
				sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
				ServerSocketFactory factory = sslContext.getServerSocketFactory();
//				System.out.println("webserver ServerController start() port: " + this.httpsPort);
	//			System.out.println("webserver ServerController start() port: " + this.httpPort);
				ServerSocket serverSocketTLS = factory.createServerSocket(this.httpsPort);
	//			ServerSocket serverSocketTLS = factory.createServerSocket(this.httpPort);
				
				Daemon httpsDaemon = new Daemon(serverSocketTLS, this.blockingQueue);
				httpsDaemon.start();
			}
//			serverSock = new ServerSocket(this.httpPort);
			
			for (int i = 0; i < NUM_WORKERS; i++) {
				ServerWorker newWorker = new ServerWorker(this.blockingQueue, this.dir);
				this.workersPool.add(newWorker);
				newWorker.start();
			}
			Daemon httpDaemon = new Daemon(serverSock, this.blockingQueue);
			httpDaemon.start();

//			Daemon httpsDaemon = new Daemon(serverSocketTLS, this.blockingQueue);
//			httpsDaemon.start();
			
			SessionCollector sessionCollector = new SessionCollector();
			sessionCollector.start();
		} catch (KeyStoreException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CertificateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnrecoverableKeyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeyManagementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
