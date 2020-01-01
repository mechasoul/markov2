package my.cute.markov2.exceptions;

public final class UncheckedFollowingWordRemovalException extends RuntimeException {

	private static final long serialVersionUID = 1L;
	private final FollowingWordRemovalException cause;

	public UncheckedFollowingWordRemovalException(FollowingWordRemovalException ex) {
		this.cause = ex;
	}
	
	public UncheckedFollowingWordRemovalException() {
		this.cause = null;
	}

	@Override
	public FollowingWordRemovalException getCause() {
		return this.cause;
	}
}
