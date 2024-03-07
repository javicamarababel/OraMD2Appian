package oramd.sqldeveloper;

public class MdlParseException extends Exception {

	public MdlParseException(String message) {
		super(message);
	}
	public MdlParseException(String message,Throwable e) {
		super(message,e);
	}
}
