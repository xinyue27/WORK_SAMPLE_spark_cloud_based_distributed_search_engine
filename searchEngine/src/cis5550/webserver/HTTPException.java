package cis5550.webserver;

public class HTTPException extends Exception{
	int errCode;

	public HTTPException(int errCode) {
		this.errCode = errCode;
	}
	public int errCode() {
		return errCode;
	}
}
