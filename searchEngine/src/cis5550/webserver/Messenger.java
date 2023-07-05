package cis5550.webserver;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;

public class Messenger {

	public void Messenger() {

	}

	public void sendDynamicResponse(Socket sock, ResponseImpl response) {
//		System.out.println("sending dynamic reseponse.");
		
		try {
//			BufferedOutputStream sout = new BufferedOutputStream(sock.getOutputStream());
			PrintWriter pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(sock.getOutputStream())), true);
			StringBuilder sb = new StringBuilder();
			sb.append("HTTP/1.1 ");
			sb.append(response.getStatusCode()).append(" ");
			sb.append(response.getReasonPhrase()).append("\r\n");
			response.header("server", response.getHost());
//			response.header("Server", "localhost");
			response.header("Content-Type", response.getContentType());
//			sb.append("Server: localhost\r\n");
//			sb.append("Content-Type: ");
//
//			sb.append(response.getContentType());

			for (String header : response.getHeaders()) {
//				System.out.println("header: " + header);
				sb.append(header);
				sb.append("\r\n");
			}

			sb.append("\r\n");
//			sb.append(response.getBody());
//			System.out.println("sending response: " + sb.toString());
//			sout.write(sb.toString().getBytes());

//			sout.flush();
			pw.write(sb.toString());
			pw.flush();
//			System.out.println("in messenger body as bytes: " + response.getBodyAsBytes());
//			for (byte b: response.getBodyAsBytes()) {
//				System.out.print(b + " ");
//			}
//			System.out.println();
			sock.getOutputStream().write(response.getBodyAsBytes(), 0, response.getBodyAsBytes().length);
//			System.out.println("messenger sending off response: " + sb.toString());
//			System.out.println("messenger sending body: " + response.getBody());

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void sendBytes(Socket sock, byte[] b) {
//		System.out.println("messenger sending just bytes");
		try {
//			BufferedOutputStream sout = new BufferedOutputStream(sock.getOutputStream());
//
//			sout.write(b);
//			sout.flush();
//			sock.close();
			sock.getOutputStream().write(b);
			sock.getOutputStream().flush();
//			ByteArrayOutputStream bs = new ByteArrayOutputStream();
//			bs.write(b);
//			bs.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void sendStaticResponse(Socket sock, RequestImpl request, ResponseImpl response, String dir) {
//		System.out.println("sendStaticResponse.");
		try {

			BufferedOutputStream sout = new BufferedOutputStream(sock.getOutputStream());

			StringBuilder sb = new StringBuilder();
			sb.append("HTTP/1.1 200 OK\r\n");
			response.status(200, "OK");
			response.header("Server", "localhost");
//			sb.append("Server: localhost\r\n");

			for (String header : response.getHeaders()) {
				sb.append(header);
				sb.append("\r\n");
			}
			sb.append("\r\n");

			if (request.requestMethod().equals("GET")) {

				sout.write(sb.toString().getBytes());
				sout.write(response.getBodyAsBytes());

			} else if (request.requestMethod().equals("HEAD")) {

				sout.write(sb.toString().getBytes());
			}
			sout.flush();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("ERR: error getting sock output stream during sending responses.");
			e.printStackTrace();
		}
	}

	public void sendException(Socket sock, RequestImpl request, ResponseImpl response) {
//		System.out.println("sendingException");
		int errCode = response.getStatusCode();
		String reasonPhrase = response.getReasonPhrase();
		String uri = request.url() == null ? "" : request.url();
		String contentType = request.contentType();
		
		PrintWriter out;
		try {
			out = new PrintWriter(sock.getOutputStream());
			StringBuilder sb = new StringBuilder();
			sb.append("HTTP/1.1 ");
			sb.append(errCode);
			sb.append(" ");
			sb.append(reasonPhrase);
			sb.append("\r\n");
			response.status(errCode, reasonPhrase);
			sb.append("server: localhost\r\n");
			response.header("Server", "localhost");
			sb.append("ContentType: ");
			sb.append(contentType);
			sb.append("\r\nContent-Length: ");
			sb.append(reasonPhrase.length());
			response.header("Content-Type", contentType);
			response.header("Content-Length", reasonPhrase.length() + "");
			sb.append("\r\n\r\n");
			sb.append(reasonPhrase);
			out.print(sb.toString());
			out.flush();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("ERR: error getting sock output stream during sending exceptions.");
			e.printStackTrace();
		}

	}

}
