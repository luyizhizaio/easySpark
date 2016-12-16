package com.streaming.exception;

public class RSAAlgorithmException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public RSAAlgorithmException() {
		super();
	}

	public RSAAlgorithmException(String message, Throwable cause) {
		super(message, cause);
	}

	public RSAAlgorithmException(String message) {
		super(message);
	}

	public RSAAlgorithmException(Throwable cause) {
		super(cause);
	}
	
}
