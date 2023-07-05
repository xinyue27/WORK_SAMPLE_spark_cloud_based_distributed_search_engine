package cis5550.webserver;

import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class ResponseImpl implements Response {
	private Socket sock;
	private byte[] bodyAsBytes;
	private List<String> headers;
//	private String contentType;
	int statusCode;
	String reasonPhrase;
	private String host;
	private Messenger messenger;

	public ResponseImpl() {
		this.headers = new ArrayList<String>();
		this.bodyAsBytes = new byte[0];
		this.statusCode = 200;
		this.reasonPhrase = "OK";
		this.host = "localhost";
		this.messenger = new Messenger();
	}

	public ResponseImpl(Socket sock) {
		this.headers = new ArrayList<String>();
		this.bodyAsBytes = new byte[0];
		this.statusCode = 200;
		this.reasonPhrase = "OK";
		this.sock = sock;
		this.host = "localhost";
		this.messenger = new Messenger();
	}

	@Override
	public void body(String body) {
		if (!this.isCommited()) {
			this.bodyAsBytes = body.getBytes();
		}
	}

	@Override
	public void bodyAsBytes(byte[] bodyArg) {
		if (!this.isCommited()) {
			this.bodyAsBytes = bodyArg;
			
		}
	}

	@Override
	public void header(String name, String value) {
		this.headers.add(name + ": " + value);
	}

	@Override
	public void type(String contentType) {
		this.headers.add("Content-Type: " + contentType);
		System.out.println("adding header contentTYpe.");
	}

	@Override
	public void status(int statusCode, String reasonPhrase) {
		// TODO Auto-generated method stub
		if (!this.isCommited()) {
			this.statusCode = statusCode;
			this.reasonPhrase = reasonPhrase;
		}
	}

	@Override
	public void write(byte[] b) throws Exception {		
		this.appendBody(b);
		if (this.isCommited()) {
			this.messenger.sendBytes(sock, b);
		} else {
			this.header("Connection", "close");
			this.messenger.sendDynamicResponse(sock, this);
		}

	}
	
	// EC2: Redirection.
	public void redirect(String url) {
		//default to 301.
		this.redirect(url, 301);
	}

	// EC2: Redirection.
	@Override
	public void redirect(String url, int responseCode) {
		this.header("Location", url);
		this.status(responseCode, "Redirect");
	}

	// EC3: filters
	@Override
	public void halt(int statusCode, String reasonPhrase) {
		this.status(statusCode, reasonPhrase);
	}


	public boolean isCommited() {
		return this.getHeaders().contains("Connection: close");
	}
	
	private void appendBody(byte[] b) {
		if (!isCommited()) {
			this.bodyAsBytes(b);
			return;
		}
		
		byte[] newArr = new byte[this.getBodyAsBytes().length + b.length];

		System.arraycopy(this.getBodyAsBytes(), 0, newArr, 0, this.getBodyAsBytes().length);

		System.arraycopy(b, 0, newArr, this.getBodyAsBytes().length, b.length);
		this.bodyAsBytes = newArr;
	}
	
	public byte[] getBodyAsBytes() {
		return this.bodyAsBytes;
	}

	public String getBody() {
		return new String(this.bodyAsBytes);
	}

	public List<String> getHeaders() {
		return this.headers;
	}
	
	public int getStatusCode() {
		return this.statusCode;
	}
	
	public String getReasonPhrase() {
		return this.reasonPhrase;
	}
	
	public String getContentType() {
		for (String header: this.headers) {
			
			if (header.toLowerCase().startsWith("content-type: ")) {
				
				String contentType = header.substring(header.indexOf(":") + 2);
				
				return contentType;
			}
		}
//		return "application/octet-stream";
		return "text/html";
	}
	
	public String getHost() {
		return this.host;
	}
	public void setHost(String host) {
		this.host = host;
	}


}
