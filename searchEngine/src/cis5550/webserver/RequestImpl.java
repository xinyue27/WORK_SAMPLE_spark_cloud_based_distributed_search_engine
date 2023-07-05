package cis5550.webserver;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class RequestImpl implements Request {
	String method;
	String url;
	String protocol;
	InetSocketAddress remoteAddr;
	Map<String, String> headers;
	Map<String, String> queryParams;
	Map<String, String> params;
	byte bodyRaw[];
	Server server;
	private SessionImpl session;
	

	RequestImpl(String methodArg, String urlArg, String protocolArg, Map<String, String> headersArg,
			Map<String, String> queryParamsArg, Map<String, String> paramsArg, InetSocketAddress remoteAddrArg,
			byte bodyRawArg[], Server serverArg) {
		method = methodArg;
		url = urlArg;
		remoteAddr = remoteAddrArg;
		protocol = protocolArg;
		headers = headersArg;
		queryParams = queryParamsArg;
		params = paramsArg;
		bodyRaw = bodyRawArg;
		server = serverArg;
		
	}

	@Override
	public String ip() {
		// TODO Auto-generated method stub
		return remoteAddr.getAddress().getHostAddress();
	}

	@Override
	public int port() {
		// TODO Auto-generated method stub
		return remoteAddr.getPort();
	}

	@Override
	public String requestMethod() {
		// TODO Auto-generated method stub
		return method;
	}

	@Override
	public String url() {
		// TODO Auto-generated method stub
		return url;
	}

	@Override
	public String protocol() {
		// TODO Auto-generated method stub
		return protocol;
	}

	@Override
	public Set<String> headers() {
		// TODO Auto-generated method stub
		return headers.keySet();
	}

	@Override
	public String headers(String name) {
		// TODO Auto-generated method stub
		return headers.get(name.toLowerCase());
	}

	@Override
	public String contentType() {
		// TODO Auto-generated method stub
		return headers.get("content-type");
	}

	@Override
	public String body() {
		// TODO Auto-generated method stub
		return new String(bodyRaw, StandardCharsets.UTF_8);
	}

	@Override
	public byte[] bodyAsBytes() {
		// TODO Auto-generated method stub
		return bodyRaw;
	}

	@Override
	public int contentLength() {
		// TODO Auto-generated method stub
		return bodyRaw.length;
	}

	@Override
	public Set<String> queryParams() {
		// TODO Auto-generated method stub
		return queryParams.keySet();
	}

	@Override
	public String queryParams(String param) {
		// TODO Auto-generated method stub
		if (queryParams == null) {
			return null;
		}
		if (queryParams.containsKey(param)) {
			return queryParams.get(param);
		}
		return null;
	}

	@Override
	public Map<String, String> params() {
		// TODO Auto-generated method stub
		return params;
	}

	@Override
	public String params(String name) {
		// TODO Auto-generated method stub
		return params.get(name);
	}

	@Override
	public Session session() {
		if (this.session == null) {
			String sessionId = UUID.randomUUID().toString();
			SessionImpl session = new SessionImpl(sessionId);

			this.session = session;
			Server.sessionsTable.put(sessionId, session);
			return session;
		} else {
			return this.session;
		}
	}

	public void setParams(Map<String, String> params) {
		this.params = params;
	}

	public void setQueryParams(Map<String, String> queryParams) {
		this.queryParams = queryParams;
	}
	
	public void setSession(SessionImpl session) {
		this.session = session;
	}
	
	public Session getSession() {
		return this.session;
	}
	
	public Map<String, String> getHeaders() {
		return this.headers;
	}

	@Override
	public String toString() {
		return "RequestImpl [method=" + method + ", url=" + url + ", protocol=" + protocol + ", remoteAddr="
				+ remoteAddr + ", headers=" + headers + ", queryParams=" + queryParams + ", params=" + params
				+ ", server=" + server + "]";
	}
	

}
