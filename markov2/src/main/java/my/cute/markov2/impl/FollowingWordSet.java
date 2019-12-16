package my.cute.markov2.impl;

import java.io.IOException;
import java.util.Random;

import org.nustaq.serialization.FSTBasicObjectSerializer;
import org.nustaq.serialization.FSTClazzInfo;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;
import org.nustaq.serialization.FSTClazzInfo.FSTFieldInfo;

/*
 * represents the set of words that follow a given bigram in the database
 * has a 1-to-1 relationship with bigram
 */
interface FollowingWordSet {
	
	public static enum Type {
		SMALL(0),
		LARGE(1);
		
		private int value;
		
		Type(int val) {
			this.value = val;
		}
		
		public int getValue() {
			return this.value;
		}
		
		public static Type fromInt(int i) {
			for(Type type : Type.values()) {
				if(type.getValue() == i) return type;
			}
			throw new IllegalArgumentException("no FollowingWordSet.Type exists with value " + i);
		}
	}
	
	static class Serializer extends FSTBasicObjectSerializer {
		
		/*
		 * important note: NOT using specific serializers for each fws type here, 
		 * because doing so means that we use the same serializer to read back from
		 * disk and consequently have to build the fws entirely from read data that was on disk
		 * current implementation needs a reference to the bigram the fws is connected to 
		 * (eg for hashcode), and the readObject()/instantiate() methods are limited
		 * in what data can be passed to them, so we need to construct fws from elsewhere
		 * so write the type of fws and then the raw data to disk here, then never actually
		 * reconstruct a fws directly (eg with FollowingWordSet fws = in.readObject(FolowingWordSet.class)),
		 * and instead use the type to build the inner fws dataset and construct it w/ its
		 * bigram passed in (see DatabaseWrapper.Serializer.instantiate())
		 * 
		 * some implementation stuff that im not happy with is necessary as a result (eg getType(), 
		 * writeToOutput() in fws interface), and i guess these could be avoided by eg testing 
		 * instanceof in writeObject() and casting to the correct result, but i think thats even
		 * uglier than this current solution. maybe theres a better way but for now this is fine
		 */
		@Override
		public void writeObject(FSTObjectOutput out, Object toWrite, FSTClazzInfo clzInfo, FSTFieldInfo referencedBy,
				int streamPosition) throws IOException {
			
			FollowingWordSet fws = (FollowingWordSet) toWrite;
			out.writeInt(fws.getType().getValue());
			fws.writeToOutput(out);
		}
		
		@Override
	    public void readObject(FSTObjectInput in, Object toRead, FSTClazzInfo clzInfo, FSTClazzInfo.FSTFieldInfo referencedBy)
	    {
			throw new UnsupportedOperationException("don't use FollowingWordSet.Serializer to directly deserialize!");
	    }
		
		@Override
		public Object instantiate(@SuppressWarnings("rawtypes") Class objectClass, FSTObjectInput in, FSTClazzInfo serializationInfo, FSTClazzInfo.FSTFieldInfo referencee, int streamPosition) throws Exception 
		{
			throw new UnsupportedOperationException("don't use FollowingWordSet.Serializer to directly deserialize!");
		}
		
	}

	static final Random RANDOM = new Random();
	/*
	 * adds an instance of the given word to the set
	 */
	public void addWord(String word);
	
	/*
	 * gets a randomly chosen word from the set, weighted by frequency
	 */
	public String getRandomWeightedWord();
	
	/*
	 * gets size of the set (equivalent to the number of times addWord() has been called)
	 */
	public int size();
	
	/*
	 * returns true if the given followingWord has been recorded at least once for this
	 * followingwordset. performance may be O(n) on size of set, so should be used 
	 * sparingly
	 */
	public boolean contains(String followingWord);
	
	/*
	 * removes one instance of the given followingWord being used for this set. if the
	 * given followingWord doesn't exist, the set will be unchanged
	 * returns true if the set changed as a result of this call
	 * performance may be O(n), so should be used sparingly
	 */
	public boolean remove(String followingWord);
	
	/*
	 * returns true if this set is empty
	 */
	public boolean isEmpty();
	
	public Bigram getBigram();
	
	public String getId();
	
	/*
	 * i kind of dislike putting the following two methods in this interface because they 
	 * seem implementation-dependent and not an inherent part of what a fws is
	 * but idk it makes things work nicer i guess ??
	 */
	
	/*
	 * returns the type of the fws
	 */
	public Type getType();
	
	/*
	 * writes output of fws to disk
	 */
	public void writeToOutput(FSTObjectOutput out) throws IOException;
	
	/*
	 * returns a minimal String representation of the set's contents
	 */
	public String toStringPlain();
}
