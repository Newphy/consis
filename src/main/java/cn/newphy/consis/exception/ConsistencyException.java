package cn.newphy.consis.exception;

public class ConsistencyException extends RuntimeException {

	private static final long serialVersionUID = 2465159127395780987L;

	public ConsistencyException() {
		super();
	}

	public ConsistencyException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ConsistencyException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConsistencyException(String message) {
		super(message);
	}

	public ConsistencyException(Throwable cause) {
		super(cause);
	}

	
}