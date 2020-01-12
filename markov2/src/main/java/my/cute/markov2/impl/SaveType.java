package my.cute.markov2.impl;

/*
 * method of serialization
 * SaveType.SERIALIZE uses fast-serialization library
 * SaveType.JSON uses gson
 * JSON currently unsupported
 * nest this somewhere?
 */
public enum SaveType {
	SERIALIZE,
	JSON
}
