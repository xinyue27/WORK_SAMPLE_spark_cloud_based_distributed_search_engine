package cis5550.webserver;


import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SessionImpl implements Session {
	private String sessionId;
	private int maxActiveInterval;
	long creationTime;
	long lastAccesssedTime;
	private Map<String, Object> attributes;
	
	public SessionImpl(String sessionId) {
		this.sessionId = sessionId;
		long currentTime = System.currentTimeMillis();

		this.creationTime = currentTime;
		this.lastAccesssedTime = this.creationTime;
		this.maxActiveInterval = 300;
		this.attributes = new HashMap<String, Object>();
	}

	@Override
	public String id() {
		// TODO Auto-generated method stub
		return this.sessionId;
	}

	@Override
	public long creationTime() {
		// TODO Auto-generated method stub
		return this.creationTime;
	}

	@Override
	public long lastAccessedTime() {
		// TODO Auto-generated method stub
		return this.lastAccesssedTime;
	}

	@Override
	public void maxActiveInterval(int seconds) {
		// TODO Auto-generated method stub
		this.maxActiveInterval = seconds;
	}

	@Override
	public void invalidate() {
		// TODO Auto-generated method stub
		Map<String, SessionImpl> sessions = Server.sessionsTable;
		for (String key: sessions.keySet()) {
			if (key.equals(this.id())) {
				sessions.remove(key);
			}
		}
	}

	@Override
	public Object attribute(String name) {
		// TODO Auto-generated method stub
		return this.attributes.get(name);
	}

	@Override
	public void attribute(String name, Object value) {
		// TODO Auto-generated method stub
		this.attributes.put(name, value);
	}
	
	public void refreshSession() {
        

        
		
		
		this.lastAccesssedTime = System.currentTimeMillis();
		
		
		
		
	}
	
	public boolean isSessionNewlyCreated() {
		return this.creationTime == this.lastAccesssedTime;
	}
	
	public boolean isSessionExpired() {

		long currTimestamp = System.currentTimeMillis();
		if (currTimestamp - this.lastAccesssedTime > this.maxActiveInterval * 1000) {
			
			return true;
		}
		return false;
	}
	
	public Map<String, Object> getAttributes() {
		return this.attributes;
	}
	
	

	public long getLastAccesssedTime() {
		return lastAccesssedTime;
	}

	public void setLastAccesssedTime(long lastAccesssedTime) {
		this.lastAccesssedTime = lastAccesssedTime;
	}

	public int getMaxActiveInterval() {
		return maxActiveInterval;
	}

	@Override
	public String toString() {
		return "SessionImpl [sessionId=" + sessionId + ", maxActiveInterval=" + maxActiveInterval + ", creationTime="
				+ creationTime + ", lastAccesssedTime=" + lastAccesssedTime + ", attributes=" + attributes + "]";
	}
	

}
