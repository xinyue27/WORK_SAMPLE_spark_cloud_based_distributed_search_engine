package cis5550.webserver;

import java.util.Map;

public class SessionCollector extends Thread{
	public SessionCollector() {
		
	}
	
	public void run() {
		while (true) {
			try {
				sleep(1);
				Map<String, SessionImpl> sessionsTable = Server.sessionsTable;
				for (String sessionId: sessionsTable.keySet()) {
					SessionImpl session = sessionsTable.get(sessionId);
					if (session.isSessionExpired()) {
//						System.out.println("SessionCollector: this session has expried: " + session);
						session.invalidate();
					}
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}


			
		}
	}
}
