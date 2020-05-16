package my.cute.markov2.exceptions;

import java.io.IOException;

public final class ReadObjectException extends IOException {

	private static final long serialVersionUID = 1L;

	public ReadObjectException(String message) {
		super(message);
	}

	public ReadObjectException(Throwable cause) {
		super(cause);
	}

}