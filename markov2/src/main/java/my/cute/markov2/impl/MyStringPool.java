package my.cute.markov2.impl;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

public enum MyStringPool implements Interner<String> {
	
	INSTANCE;
	
	private final Interner<String> interner = Interners.newWeakInterner();

	@Override
	public String intern(String sample) {
		return interner.intern(sample);
	}

}