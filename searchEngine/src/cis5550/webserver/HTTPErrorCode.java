package cis5550.webserver;

public class HTTPErrorCode {
	private int errCode;
	private String errMessage;
	public HTTPErrorCode(int errCode) {
		this.errCode = errCode;
	}
	public String getErrMessage(int errCode) {
		switch(errCode) {
		case 400:
			return "Bad Request";
		case 403:
			return "Forbidden";
		case 404:
			return "Not Found";
		case 405:
			return "Not Allowed";
		case 501:
			return "Not Implemented";
		case 505:
			return "HTTP Version Not Supported";
		case 304:
			return "Not Modified";
		case 500:
			return "Internal Server Error";
		default:
			return "Unknown Error";
		}
	}
}
