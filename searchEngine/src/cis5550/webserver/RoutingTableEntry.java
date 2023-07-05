package cis5550.webserver;

public class RoutingTableEntry {
	private String method;
	private String uri;
	private Route route;
	private String host;

	public RoutingTableEntry() {

	}

	public RoutingTableEntry(String method, String uri, Route route, String host) {
		super();
		this.method = method;
		this.uri = uri;
		this.route = route;
		this.host = host;
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public Route getRoute() {
		return route;
	}

	public void setRoute(Route route) {
		this.route = route;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	@Override
	public String toString() {
		return "RoutingTableEntry [method=" + method + ", uri=" + uri + ", route=" + route + ", host=" + host + "]";
	}

}
