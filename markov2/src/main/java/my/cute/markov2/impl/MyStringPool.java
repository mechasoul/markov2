package my.cute.markov2.impl;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

/*
 * used as String.intern() replacement
 * even with newer java versions' ability to set size of string pool,
 * i had significant performance degradation over a large number of 
 * operations? guava interner performed much better and allows for
 * weak references, preserving valuable memory
 * enum singleton pattern is used since our pool is project-wide
 */
enum MyStringPool implements Interner<String> {
	
	INSTANCE;
	
	private final Interner<String> interner = Interners.newWeakInterner();

	@Override
	public String intern(String sample) {
		return interner.intern(sample);
	}

}