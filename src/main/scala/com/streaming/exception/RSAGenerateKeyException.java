package com.streaming.exception;

public class RSAGenerateKeyException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public RSAGenerateKeyException() {
		super();
	}

	public RSAGenerateKeyException(String message, Throwable cause) {
		super(message, cause);
	}

	public RSAGenerateKeyException(String message) {
		super(message);
	}

	public RSAGenerateKeyException(Throwable cause) {
		super(cause);
	}

}
