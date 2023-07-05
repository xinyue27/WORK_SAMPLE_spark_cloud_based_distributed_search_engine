package cis5550.kvs;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.Map;

public class FlushThread extends Thread{
	private Map<String, BufferedOutputStream> workerTablesLog;

	public FlushThread(Map<String, BufferedOutputStream> workerTablesLog) {
		super();
		this.workerTablesLog = workerTablesLog;
	}
	
	public void run() {
		while (true) {
			try {
				sleep(5000);
				for (String tableName: workerTablesLog.keySet()) {
					BufferedOutputStream out = workerTablesLog.get(tableName);
					if (out != null) {
						out.flush();	
					}
				}
			} catch (InterruptedException | IOException e) {
				e.printStackTrace();
			}
		}
	}
}
