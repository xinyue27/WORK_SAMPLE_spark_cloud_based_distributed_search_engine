package cis5550.kvs;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class PingThread extends Thread {
	private String workerId;
	private int workerPort;
	private String masterIpAndPort;

	
	
	public PingThread(String workerId, int workerPort, String masterIpAndPort) {
		super();
		this.workerId = workerId;
		this.workerPort = workerPort;
		this.masterIpAndPort = masterIpAndPort;
	}
	
	public void run() {
		while (true) {
			try {
				sleep(5000);
				StringBuilder sb = new StringBuilder();
				sb.append("http://");
				sb.append(this.masterIpAndPort);
				sb.append("/ping?id=");
				sb.append(this.workerId);
				sb.append("&port=");
				sb.append(this.workerPort);
				URL url = new URL(sb.toString());
				Object content = url.getContent();

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
