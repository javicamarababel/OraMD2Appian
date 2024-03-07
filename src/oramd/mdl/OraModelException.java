package oramd.mdl;

public class OraModelException extends Exception {

	public OraModelException(String message) {
		super(message);
	}

	public OraModelException(String message, Throwable cause) {
		super(message, cause);
	}
}
