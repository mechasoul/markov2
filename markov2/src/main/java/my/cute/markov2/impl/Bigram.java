package my.cute.markov2.impl;

import java.io.Serializable;


public class Bigram implements Serializable {

	private static final long serialVersionUID = 1L;
	private final String word1;
	private final String word2;
	
	public Bigram(String w1, String w2) {
		word1 = w1.intern();
		word2 = w2.intern();
	}
	
	public String getWord1() {
		return this.word1;
	}
	
	public String getWord2() {
		return this.word2;
	}
	
	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((word1 == null) ? 0 : word1.hashCode());
		result = prime * result + ((word2 == null) ? 0 : word2.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof Bigram)) {
			return false;
		}
		Bigram other = (Bigram) obj;
		if (word1 == null) {
			if (other.word1 != null) {
				return false;
			}
		} else if (!word1.equals(other.word1)) {
			return false;
		}
		if (word2 == null) {
			if (other.word2 != null) {
				return false;
			}
		} else if (!word2.equals(other.word2)) {
			return false;
		}
		return true;
	}

	public String toString() {
		return "Bigram(" + this.word1 + ", " + this.word2 + ")";
	}
}
