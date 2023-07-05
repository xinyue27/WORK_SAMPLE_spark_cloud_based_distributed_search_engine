package cis5550.webserver;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;

public class ServerWorker extends Thread {
	private BlockingQueue<Socket> blockingQueue;
	private String dir;
	private Socket sock;
	private Messenger messenger;
	private final static String SERVERNAME = "XinyueServer";

	public ServerWorker(BlockingQueue<Socket> blockingQueue, String dir) {
		this.blockingQueue = blockingQueue;
		this.dir = dir;
		this.messenger = new Messenger();
	}

	public void run() {
		while (true) {
			try {
				Socket sock = blockingQueue.take();
				this.sock = sock;

//				InputStream inputStream = new BufferedInputStream(this.sock.getInputStream());
				InputStream inputStream = this.sock.getInputStream();
				boolean hasReachedEof = false;
				while (!hasReachedEof && !sock.isClosed()) {
					hasReachedEof = handleRequest(inputStream);
				}

				if (!sock.isClosed()) {
					this.sock.close();
				}

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				System.out.println("ERR: unable to take a sock from blocking queue.");
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("ERR: server worker error during handling request.");
				e.printStackTrace();
			}
		}
	}

	public boolean handleRequest(InputStream inputStream) throws IOException, InterruptedException {
//		System.out.println("ServerWorker " + this + " is working.");
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();

		int matchedPtr = 0;

		boolean hasReachedEof = false;
		while (matchedPtr < 4) {
			int b = inputStream.read();
			if (b < 0) {
				// reached EOF
				hasReachedEof = true;
				return hasReachedEof;
			}
			buffer.write(b);

			if ((b == '\r' && (matchedPtr == 0 || matchedPtr == 2))
					|| (b == '\n' && (matchedPtr == 1 || matchedPtr == 3))) {
				matchedPtr++;
			} else {
				matchedPtr = 0;
			}
		}
		// by now we have seen \r\n\r\n.
		// buffer has everything before and including \r\n\r\n.
		BufferedReader hbr = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buffer.toByteArray())));
		Map<String, String> requestHeaders = new HashMap<String, String>();
		try {
			requestHeaders = parseHeader(hbr);
//			 System.out.println("headers returned: " + requestHeaders);

			ByteArrayOutputStream body = new ByteArrayOutputStream();
			int contentLen = 0;
			if (requestHeaders.containsKey("content-length")) {
				contentLen = Integer.parseInt(requestHeaders.get("content-length"));
			}
			for (int i = 0; i < contentLen; i++) {
				int b = inputStream.read();
				if (b < 0) {
					// reached EOF
//					System.out.println("b < 0, reached eof.");
					hasReachedEof = true;
					return hasReachedEof;
				}
				body.write(b);

			}
			requestHeaders.put("content-length", body.toString().length()+"");
//			System.out.println("body: " + body.toString());
			
			String method = requestHeaders.get("method");
			String uri = requestHeaders.get("uri");
			String protocol = requestHeaders.get("protocol");
			String host = requestHeaders.get("host");
//			byte[] bodyAsBytes = body.toString().getBytes();
			byte[] bodyAsBytes = body.toByteArray();
//			System.out.println("what is in server body? " + bodyAsBytes);
//			for (byte b: bodyAsBytes) {
//				System.out.print(b + " ");
//			}
//			System.out.println();

			// I'm persisting to the implementation provided in RequestImpl,
			// but setting server args as null, I'm not using them.
//			InetSocketAddress remoteAddr = new InetSocketAddress(host, this.sock.getPort());
			
			InetSocketAddress remoteAddr = (InetSocketAddress)this.sock.getRemoteSocketAddress();
			RequestImpl request = new RequestImpl(method, uri, protocol, requestHeaders, null, null, remoteAddr, bodyAsBytes, null);
			ResponseImpl response = new ResponseImpl(this.sock);
			
			// Check BEFORE filter here
			try {
				handleBeforeFilters(request, response);
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				System.out.println("ERR: filter handler exception.");
				// errcode and message already set in response.halt().
				this.messenger.sendException(sock, request, response);
				return true;
			}
			
			Map<String, SessionImpl> sessionsTable = Server.sessionsTable;
//			System.out.println("sessions map: " + sessionsTable);

			/*
			 * if requestHeaders contains key "cookie" {
			 *   This means that the request has a Cookie:SessionId=**** header.
			 *   String sessionId = parse the sessionId from requestHeaders.get("cookie").
			 *   if (sessionsTable contains key sessionId) {
			 *       This means that this sessionId exists in SessionTable.
			 *       SessionImpl session = sessionTable.get(sessionId);
			 *       if (session is not expired) {
			 *          Server should only do something at this point. setSession() and refreshSession().
			 *       } else {
			 *          session expired, do nothing. if session() is called, it will create a new one.
			 *       }
			 *   } else {
			 *       This means that the request has a Cookie:SessionId=**** header, but this sessionId does NOT exist in SessionTable.
		     *       Not doing anything in server, if a client calls session(), the function will create a new session.
			 *   }
			 *   
			 * } else {
			 *   This means that the request has no cookie header.
			 *   In this case, the server does nothing.
			 *   If a client calls session(), the function will create a new session.
			 * }
			 */
			if (requestHeaders.containsKey("cookie")) {
				String sessionId = requestHeaders.get("cookie").substring("SessionID=".length());
//				System.out.println("sessionId in header: " + sessionId);
				SessionImpl session = sessionsTable.get(sessionId);
				if (sessionsTable.containsKey(sessionId) && !session.isSessionExpired()) {
					request.setSession(session);
					session.refreshSession();
				}
			}
			
			
			boolean foundMatch = handleDynamicRequests(request, response);
			if (!foundMatch) {
				if (this.dir == null) {
					System.out.println("ERR: static file dir location not specified. Not handling this request.");

				} else {

					handleStaticRequests(request, response);

				}
			}
		} catch (HTTPException e) {
			String methodArg = requestHeaders.containsKey("method") ? requestHeaders.get("method") : "";
			String urlArg = requestHeaders.containsKey("uri") ? requestHeaders.get("uri") : "";
			String protocolArg = requestHeaders.containsKey("protocol") ? requestHeaders.get("protocol") : "";
			RequestImpl request = new RequestImpl(methodArg, urlArg, protocolArg, requestHeaders, null, null, null, null,
					null);
			
			ResponseImpl response = new ResponseImpl();
			int errCode = e.errCode;
			HTTPErrorCode ec = new HTTPErrorCode(errCode);
			response.status(errCode, ec.getErrMessage(errCode));
			this.messenger.sendException(sock, request, response);
		}

		return hasReachedEof;
	}

	private Map<String, String> parseHeader(BufferedReader headerIn) throws HTTPException {
		// header names are not case sensitive.
		// the header contents should be case sensitive.
		// "I would treat them as case-sensitive, but that would mean that you’d NOT
		// want to regularize the case - ‘foo’ and ‘Foo’ would be different parameters."
		Map<String, String> headers = new HashMap<String, String>();
		try {
			// preheaders are in first line.
			String s = headerIn.readLine();
			if (s == null) {
				return headers;
			}
			StringTokenizer st = new StringTokenizer(s);
			if (!st.hasMoreTokens()) {
				System.out.println("ERR: request has no method");
				throw new HTTPException(400);
			}

			String method = st.nextToken();

			headers.put("method", method);
			if (!st.hasMoreTokens()) {
				System.out.println("ERR: request has no uri");
				throw new HTTPException(400);
			}
			String uri = st.nextToken();
			headers.put("uri", uri);
			headers.put("content-type", getContentType(uri));
			if (!st.hasMoreTokens()) {
				System.out.println("ERR: request has no protocol");
				// return headers;
				throw new HTTPException(400);
			}

			headers.put("protocol", st.nextToken());

			String line = headerIn.readLine();
			while (line != null && !line.trim().isEmpty()) {
				int p = line.indexOf(":");
				if (p > 0) {
					headers.put(line.substring(0, p).trim().toLowerCase(), line.substring(p + 1).trim());
				}
				line = headerIn.readLine();
			}

			// System.out.println("headers: " + headers);
			Set<String> validMethods = new HashSet<String>();
			validMethods.add("get");
			validMethods.add("head");
			validMethods.add("put");
			validMethods.add("delete");
			validMethods.add("post");
			if (!validMethods.contains(method.toLowerCase())) {
				System.out.println("ERR: request has invalid method");
				throw new HTTPException(501);
			}
			// if (method.equals("post") || method.equals("put")) {
			// System.out.println("ERR: not allowed method POST or PUT");
			//// return headers;
			// throw new HTTPException(405);
			// }
			if (!headers.containsKey("host")) {
				System.out.println("ERR: request has no host");
				throw new HTTPException(400);
			}
			if (!headers.get("protocol").toLowerCase().equals("http/1.1")) {
				System.out.println("ERR: request has bad protocol version.");
				throw new HTTPException(505);
			}

			return headers;

		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("ERR: error in server worker during parsing headers.");
			return null;
		}

	}

	/**
	 * Handle dynamic requests.
	 * 
	 * @param headers request headers
	 * @param body    request body
	 * @return true if we should handle this request as dynamic request, false if we
	 *         should treat it as static request.
	 */
//	private boolean handleDynamicRequests(Map<String, String> headers, byte[] body) {
	private boolean handleDynamicRequests(RequestImpl request, ResponseImpl response) {

//		 System.out.println("handling dynamic requests, request headers: " + request.getHeaders());
		 Map<String, String> requestHeaders = request.getHeaders();
		 String requestUri = requestHeaders.get("uri");

		
		Route matchedRoute = match(request);
//		System.out.println("matched route: " + matchedRoute);
//		System.out.println("request: " + request);

		// checking if a given request url matches with one in routing table.
		if (matchedRoute != null) {
//			 System.out.println("this route is registered by client, should handle dynamic request instead of static file");

			try {

				Object obj = matchedRoute.handle(request, response);
//				 System.out.println("obj: " + obj);

				if (requestUri.equals("/write")) {
					// the response handling is done in ResponseImpl directly as said in edstem:
					// "you need to send the headers while the route handler is still running"
					// System.out.println("write response body: " + response.getBody());

					// directly return, since the response has already been sent when a client calls
					// res.write().
					// return true because we don't want this to match static request handler.

					return true;
				}

				if (obj != null) {
					// if obj is not null, attach the obj to response body.
					// System.out.println("handler returning obj: " + new
					// String(obj.toString().getBytes()));
//					 System.out.println("obj to string: " + obj.toString());
					response.body(obj.toString());
					response.header("Content-Length", obj.toString().length() + "");

				} else {
					// if obj is null
//					System.out.println("router returned obj is null");
					int contentLength = response.getBody().equals("") ? 0 : response.getBody().length();

					response.header("Content-Length", contentLength + "");

				}
				
				response.setHost(SERVERNAME);
				
				SessionImpl session = (SessionImpl)request.getSession();
				if (session != null) {
//					System.out.println("PRINT SESSION======");
//					System.out.println(session);
//					System.out.println("PRINT SESSION DONE======");
					boolean isSessionNewlyCreated = session.isSessionNewlyCreated();
					if (isSessionNewlyCreated) {
						// this means that this session has a creationTime different from lastAccessedTime.
						// which means that the server has called refreshSession() when it found out that the session exists in the sessionTable.
						// Only the server can do a refresh, the client cannot call refreshSession()
						response.header("Set-Cookie", "SessionID=" + session.id());
					}
				}


				
				handleAfterFilters(request, response);
				this.messenger.sendDynamicResponse(this.sock, response);
				return true;
			} catch (Exception e) {
				// If a route throws any exception and write() has not been called,
				// your solution should returna500 Internal Server Errorresponse.
				// If write() has been called,it should simply close the connection.
				System.out.println("ERR: router exception during handling request.");
				e.printStackTrace();
				if (response.isCommited()) {
					return true;
				} else {

					int errCode = 500;
					HTTPErrorCode ec = new HTTPErrorCode(errCode);
					response.status(errCode, ec.getErrMessage(errCode));
					this.messenger.sendException(this.sock, request, response);

				}
				return true;
			}
		}

		return false;
	}


	private void handleStaticRequests(RequestImpl request, ResponseImpl response) {
//		 System.out.println("handle static requests, request headers: " + requestHeaders);
		Map<String, String> requestHeaders = request.getHeaders();
		String uri = requestHeaders.get("uri");


		String path = this.dir + uri;

		if (uri.contains("..")) {

			int errCode = 403;
			HTTPErrorCode ec = new HTTPErrorCode(errCode);
			response.status(errCode, ec.getErrMessage(errCode));
			this.messenger.sendException(sock, request, response);

			return;
		}

		if (ifModifiedSince(requestHeaders)) {
			return;
		}

		if (!new File(path).exists()) {
			System.out.println("ERR: requested file: " + path + " does not exist.");

			int errCode = 404;
			HTTPErrorCode ec = new HTTPErrorCode(errCode);
			response.status(errCode, ec.getErrMessage(errCode));
			this.messenger.sendException(sock, request, response);

			return;
		}

		if (!Files.isReadable(Paths.get(path))) {
			System.out.println("ERR: file is not readable.");

			int errCode = 403;
			HTTPErrorCode ec = new HTTPErrorCode(errCode);
			response.status(errCode, ec.getErrMessage(errCode));
			this.messenger.sendException(sock, request, response);

			return;
		}

		byte[] fileContent;

		try {
			fileContent = Files.readAllBytes(Paths.get(path));
			// System.out.println("fileContent read: " + new String(fileContent));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("ERR: error reading file.");

			int errCode = 404;
			HTTPErrorCode ec = new HTTPErrorCode(errCode);
			response.status(errCode, ec.getErrMessage(errCode));
			this.messenger.sendException(sock, request, response);

			return;
		}

		String contentType = "";
		if (uri.endsWith(".jpg") || uri.endsWith(".jpeg")) {
			contentType = "image/jpeg";
		} else if (uri.endsWith(".txt")) {
			contentType = "text/plain";
		} else if (uri.endsWith(".html")) {
			contentType = "text/html";
		} else {
			contentType = "application/octet-stream";
		}

		response.header("Content-Type", contentType);
		response.header("Content-Length", fileContent.length + "");
		response.bodyAsBytes(fileContent);

		try {
			handleAfterFilters(request, response);
			this.messenger.sendStaticResponse(this.sock, request, response, dir);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("ERR: error occurred during handling after filters.");
			response.status(600, "Unknown Error");
			this.messenger.sendException(this.sock, request, response);
		}

	}

	// return the matched route juding by method, uri key, and host
	private Route match(RequestImpl request) {
		// System.out.println("if match");
		List<RoutingTableEntry> routingTable = new ArrayList<RoutingTableEntry>(Server.routingTableWithHost);
		routingTable.addAll(Server.routingTableWithoutHost);
//		System.out.println("routingTable: " + routingTable);
		Map<String, String> requestHeaders = request.getHeaders();
		String method = requestHeaders.get("method");
		String requestUri = requestHeaders.get("uri");
		String requestHost = requestHeaders.get("host");
		String contentType = requestHeaders.get("content-type");
//		System.out.println("host in request: " + requestHost);
		byte[] body = request.bodyAsBytes();

		if (!ifRoutingTableHasSuchMethod(routingTable, method)) {
//			routing table has no such method
//			System.out.println("routingTable has no such method");
			return null;
		}

		Map<String, String> param = new HashMap<String, String>();

		for (RoutingTableEntry entry : routingTable) {
			String routingTableUri = entry.getUri();
			String routingTableHost = entry.getHost();

			boolean matched = true;
			

			// look for direct match
//			System.out.println("routingTableUri: " + routingTableUri);
//			System.out.println("requestUri: " + requestUri);
//			System.out.println("routingTableHost: " + routingTableHost);
//			System.out.println("requestHost: " + requestHost);
			if (routingTableUri.equals(requestUri)
					&& (routingTableHost.equals("") || routingTableHost.equals(requestHost))) {
//					return requestUri;
				return getRouteByMethodUriAndHost(method, requestUri, routingTableHost);
			}
			
//			if (requestUri.contains("?")) {
//				// this contains a query param
//				int idx = routingTableUri.indexOf(":");
//				String routingTableUriFirstPart = routingTableUri;
//				String routingTableUriSecondPart = "";
//				if (idx >= 0) {
//					routingTableUriFirstPart = routingTableUri.substring(0, idx-1);
//					routingTableUriSecondPart = routingTableUri.substring(idx);
//				}
//				
//				System.out.println("routingTableUriFirstPart: " + routingTableUriFirstPart + " routingTableUriSecondPart: " + routingTableUriSecondPart);
//				idx = requestUri.indexOf("/");
//				String requestUriFirstPart = requestUri.substring(0, idx);
//				String requestUriSecondPart = requestUri.substring(idx);
//				System.out.println("requestUriFirstPart: " + requestUriFirstPart + " requestUriSecondPart: " + requestUriSecondPart);
//				if (!routingTableUriFirstPart.equals(requestUriFirstPart)) {
//					System.out.println("requestUri: " + requestUri + " not matching with routingTableUri: " + routingTableUri);
//					continue;
//				}
//				String[] split1 = routingTableUriSecondPart.split("/");
//				String[] split2 = requestUriSecondPart.split("/");
//				System.out.println("split1: " + split1);
//				System.out.println("split2: " + split2);
//				if (split1.length != split2.length) {
//					System.out.println("requestUri: " + requestUri + " not matching(not same length) with routingTableUri: " + routingTableUri);
//					continue;
//				}
//			}

			
			int idx = routingTableUri.indexOf(":");
			String routingTableUriFirstPart = routingTableUri;
			String routingTableUriSecondPart = "";
			if (idx >= 0) {
				routingTableUriFirstPart = routingTableUri.substring(0, idx-1);
				routingTableUriSecondPart = routingTableUri.substring(idx);
			}
			
			
//			if (routingTableUri.equals("/qparam") && requestUri.startsWith("/qparam")) {
//			if (requestUri.contains("?") && requestUri.startsWith(routingTableUriFirstPart) && !routingTableUri.equals("/")) {
			if (requestUri.contains("?") && !routingTableUri.equals("/")) {
//				matched2 = true;
//				System.out.println("requestUri has query params, parsing qparams. requestUri: " + requestUri + " requestUri method: " + method + " matching with routingTableUri: " + routingTableUri);
				// headers returned: {content-length=21, protocol=http/1.1, method=get,
				// host=localhost, content-type=application/x-www-form-urlencoded,
				// uri=/qparam?par1=ex29U+X&par2=%22a7Unt%22}
				// body: par3=Dow7D&par4=BZgwa
				if (routingTableHost.equals("") || routingTableHost.equals(requestHost)) {
					Map<String, String> queryParams = new HashMap<String, String>();
					String toSplit = requestUri.substring(requestUri.indexOf("?") + 1);
					String[] tokens = toSplit.split("&");
					for (String t : tokens) {
						String key = t.split("=")[0];
						try {
							String value = URLDecoder.decode(t.split("=")[1], "UTF-8");
							queryParams.put(key, value);
						} catch (UnsupportedEncodingException e) {
							// TODO Auto-generated catch block
							System.out.println("ERR: URLDecoder decode error.");
							e.printStackTrace();
						}

					}
//					String contentType = headers.get("content-type");
//						System.out.println("contentType: " + contentType);
					if (contentType.toLowerCase().equals("application/x-www-form-urlencoded")) {

						tokens = new String(body).split("&");
						for (String t : tokens) {
							// System.out.println("t: " + t);
							String key = t.split("=")[0];
							try {
								String value = URLDecoder.decode(t.split("=")[1], "UTF-8");
								queryParams.put(key, value);
							} catch (UnsupportedEncodingException e) {
								// TODO Auto-generated catch block
								System.out.println("ERR: URLDecoder decode error.");
								e.printStackTrace();
							}
						}
					}

//					 System.out.println("queryParams: " + queryParams);
					request.setQueryParams(queryParams);
//					return getRouteByMethodUriAndHost(method, routingTableUri, routingTableHost);
//						return routingTableUri;
				}
			}

			// System.out.println("comparing u and uri");
			// System.out.println("routingTableUri: " + routingTableUri + " requestUri: " +
			// requestUri);
			String[] uSplit = routingTableUri.split("/");
			String[] uriSplit = requestUri.split("/");
			if (uSplit.length != uriSplit.length) {
				matched = false;
				continue;
			}
			int n = uSplit.length;
			for (int i = 0; i < n; i++) {
//				System.out.println("routingTableUri split: " + uSplit[i]);
//				System.out.println("requestUri split: " + uriSplit[i]);
				String requestUriFirstPart = uriSplit[i];
				int index = uriSplit[i].indexOf("?");
				if (index > 0) {
					requestUriFirstPart = uriSplit[i].substring(0, index);
				}
				if (uSplit[i].startsWith(":")) {
					param.put(uSplit[i].substring(1), requestUriFirstPart);
				} else {
					if (!uSplit[i].equals(requestUriFirstPart)) {
						param = new HashMap<String, String>();
						matched = false;
//						System.out.println("not matched. requestUri: " + requestUri + " routingTableUri: " + routingTableUri);
						continue;
					}
				}
			}
			if (matched && (routingTableHost.equals("") || routingTableHost.equals(requestHost))) {
				// System.out.println("should match by now");
				// System.out.println("parmaMap: " + param);
//				System.out.println("requestUri matches. requestUri: " + requestUri + " requestUri method: " + method + " matches with routingTableUri: " + routingTableUri);
				request.setParams(param);
				return getRouteByMethodUriAndHost(method, routingTableUri, routingTableHost);
//					return routingTableUri;
			}
		}
//		}
		// System.out.println("parmaMap: " + param);
		return null;
	}

	private Route getRouteByMethodUriAndHost(String method, String uri, String host) {
		List<RoutingTableEntry> routingTable = new ArrayList<RoutingTableEntry>(Server.routingTableWithHost);
		routingTable.addAll(Server.routingTableWithoutHost);
//		System.out.println("routingTable: " + routingTable);
		for (RoutingTableEntry entry : routingTable) {
			if (entry.getMethod().equals(method) && entry.getUri().equals(uri) && entry.getHost().equals(host)) {
				return entry.getRoute();
			}
		}
		return null;
	}

	private boolean ifRoutingTableHasSuchMethod(List<RoutingTableEntry> routingTable, String method) {
		for (RoutingTableEntry entry : routingTable) {
			if (entry.getMethod().equals(method)) {
				return true;
			}
		}
		return false;
	}

	private void handleBeforeFilters(RequestImpl request, ResponseImpl response) throws Exception {

		List<Filter> beforeFilters = Server.beforeFilters;
		for (Filter filter : beforeFilters) {
			filter.handle(request, response);
			if (response.getStatusCode() != 200) {
				throw new HTTPException(response.getStatusCode());
			}
		}
	}

	private void handleAfterFilters(RequestImpl request, ResponseImpl response) throws Exception {
		List<Filter> afterFilters = Server.afterFilters;
		for (Filter filter : afterFilters) {
			filter.handle(request, response);
		}
	}

	private boolean ifModifiedSince(Map<String, String> headers) {

		if (headers.containsKey("if-modified-since")) {
			String uri = headers.get("uri");
			String path = dir + uri;
			Path file = Paths.get(path);
			try {
				BasicFileAttributes attr = Files.readAttributes(file, BasicFileAttributes.class);
				long fileTime = attr.lastModifiedTime().toMillis();

				// System.out.println("file last modified time: " + fileTime);

				String timestampRaw = headers.get("if-modified-since");
				String timestamp = timestampRaw.substring(0, timestampRaw.length() - 4); // get rid of "GMT"
				SimpleDateFormat formatter = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss");
				formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
				Date date;
				try {
					date = formatter.parse(timestamp);
					long ifModifiedLastTime = date.getTime();
					// System.out.println("ifModifiedLastTime: " + ifModifiedLastTime);

					if (ifModifiedLastTime > fileTime) {
						String host = headers.get("host");
						InetSocketAddress remoteAddr = new InetSocketAddress(host, this.sock.getPort());
						RequestImpl request = new RequestImpl(headers.get("method"), uri, headers.get("protocol"),
								headers, null, null, remoteAddr, null, null);
						ResponseImpl response = new ResponseImpl();
						int errCode = 304;
						HTTPErrorCode ec = new HTTPErrorCode(errCode);
						response.status(errCode, ec.getErrMessage(errCode));
						this.messenger.sendException(sock, request, response);
						// sendException(304, headers);
						return true;
					}
				} catch (ParseException e) {
					return false;
				}
			} catch (IOException e1) {
				System.out.println("ERR: err reading file attributes.");
				return false;
			}

		}
		return false;
	}

	private String getContentType(String uri) {
		String contentType = "";
		if (uri.endsWith(".jpg") || uri.endsWith(".jpeg")) {
			contentType = "image/jpeg";
		} else if (uri.endsWith(".txt")) {
			contentType = "text/plain";
		} else if (uri.endsWith(".html")) {
			contentType = "text/html";
		} else {
			contentType = "application/octet-stream";
		}
		return contentType;
	}

}
