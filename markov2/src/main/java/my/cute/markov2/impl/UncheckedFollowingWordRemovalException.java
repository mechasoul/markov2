package my.cute.markov2.impl;

public class UncheckedFollowingWordRemovalException extends RuntimeException {

	private static final long serialVersionUID = 1L;
	private final FollowingWordRemovalException cause;

	public UncheckedFollowingWordRemovalException(FollowingWordRemovalException ex) {
		this.cause = ex;
	}
	
	@Override
	public FollowingWordRemovalException getCause() {
		return this.cause;
	}
}
