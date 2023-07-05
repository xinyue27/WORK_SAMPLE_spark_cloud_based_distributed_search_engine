package cis5550.kvs;

import java.util.Objects;

public class WorkerStatus {
	private String id;
	private int port;
	private String ip;
	private Long lastPingTime;
	public WorkerStatus(String id, int port, String ip) {
		super();
		this.id = id;
		this.port = port;
		this.ip = ip;
		this.lastPingTime = System.currentTimeMillis();
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}

	public Long getLastPingTime() {
		return lastPingTime;
	}
	public void setLastPingTime(Long lastPingTime) {
		this.lastPingTime = lastPingTime;
	}
	
	public boolean isActive() {
		long currentTime = System.currentTimeMillis();
		if (currentTime - this.lastPingTime > 15000) {
			return false;
		}
		return true;
	}
	
	
	@Override
	public int hashCode() {
		return Objects.hash(id);
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		WorkerStatus other = (WorkerStatus) obj;
		return Objects.equals(id, other.id);
	}
	@Override
	public String toString() {
		return "Worker [id=" + id + ", port=" + port + ", ip=" + ip + ", lastPingTime=" + lastPingTime + "]";
	}

	
}
