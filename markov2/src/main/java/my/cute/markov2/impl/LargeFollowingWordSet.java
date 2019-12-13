package my.cute.markov2.impl;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;

import org.nustaq.serialization.FSTObjectOutput;
import org.nustaq.serialization.annotations.Flat;

import gnu.trove.TCollections;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.procedure.TObjectIntProcedure;

@Flat
public class LargeFollowingWordSet implements FollowingWordSet, Serializable {
	
//	static class Serializer extends FSTBasicObjectSerializer {
//
//		@Override
//		public void writeObject(FSTObjectOutput out, Object toWrite, FSTClazzInfo clzInfo, FSTFieldInfo referencedBy,
//				int streamPosition) throws IOException {
//			
//			LargeFollowingWordSet fws = (LargeFollowingWordSet) toWrite;
//			out.writeInt(fws.numEntries());
//			synchronized(fws) {
//				try {
//					fws.forEachEntry((word, count) ->
//					{
//						out.writeUTF(word);
//						out.writeInt(count);
//						return true;
//					});
//				} catch (UncheckedIOException ex) {
//					//rethrow outside of lambda
//					throw ex.getCause();
//				}
//			}
//		}
//		
//		@Override
//	    public void readObject(FSTObjectInput in, Object toRead, FSTClazzInfo clzInfo, FSTClazzInfo.FSTFieldInfo referencedBy)
//	    {
//	    }
//		
//		@Override
//		public Object instantiate(@SuppressWarnings("rawtypes") Class objectClass, FSTObjectInput in, FSTClazzInfo serializationInfo, FSTClazzInfo.FSTFieldInfo referencee, int streamPosition) throws Exception 
//		{
//			int size = in.readInt();
//			LargeFollowingWordSet obj = new LargeFollowingWordSet(size);
//			for(int i=0; i < size; i++) {
//				String word = MyStringPool.INSTANCE.intern(in.readUTF());
//				int count = in.readInt();
//				obj.addWordWithCount(word, count);
//			}
//			in.registerObject(obj, streamPosition, serializationInfo, referencee);
//			return obj;
//		}
//	}
	
	/*
	 * extend trove procedure to rethrow exception outside of lambda
	 * janky but the best way i can think of to get the exception thrown from 
	 * a serializer's writeObject()
	 */
	@FunctionalInterface
	private static interface IOProcedure<E> extends TObjectIntProcedure<E> {
		
		@Override
		default boolean execute(E e, int b) {
			try {
				return executeIO(e, b);
			} catch (IOException ex) {
				throw new UncheckedIOException(ex);
			}
		}
		
		boolean executeIO(E e, int b) throws IOException;
	}
	
	private static final long serialVersionUID = 1L;
	private final TObjectIntMap<String> words;
	private int totalWordCount;
	private final Bigram bigram;
	private final String id;
	
//	LargeFollowingWordSet() {
//		this.totalWordCount = 0;
//		this.words = TCollections.synchronizedMap(new TObjectIntHashMap<String>(5, 0.8f));
//	}
//	
//	LargeFollowingWordSet(String firstWord) {
//		this();
//		this.words.put(firstWord, 1);
//		this.incrementTotal();
//	}
//	
	LargeFollowingWordSet(SmallFollowingWordSet set) {
		this.totalWordCount = 0;
		this.words = TCollections.synchronizedMap(new TObjectIntHashMap<String>(set.size() * 5 / 4, 0.8f));
		synchronized(set) {
			for(String word : set) {
				this.addWord(word);
				this.incrementTotal();
			}
		}
		this.bigram = set.getBigram();
		this.id = set.getId();
	}
//	
//	LargeFollowingWordSet(int size) {
//		this.totalWordCount = 0;
//		this.words = TCollections.synchronizedMap(new TObjectIntHashMap<String>(size * 5 / 4, 0.8f));
//	}
	
	LargeFollowingWordSet(TObjectIntMap<String> map, int wordCount, Bigram bigram, String id) {
		this.words = map;
		this.totalWordCount = wordCount;
		this.bigram = bigram;
		this.id = id;
	}

	@Override
	public void addWord(String word) {
		this.words.adjustOrPutValue(word, 1, 1);
		this.incrementTotal();
	}
	
	/*
	 * returns a random word from the set, weighted according to their use count
	 */
	@Override
	public String getRandomWeightedWord() {
		String chosenWord = "hello";
		int count = RANDOM.nextInt(this.totalWordCount);
		//need to synchronize when iterating over THashMap
		synchronized(this) {
			TObjectIntIterator<String> iterator = this.words.iterator();
			for(int i=0; i < this.words.size(); i++) {
				iterator.advance();
				if(count < iterator.value()) {
					chosenWord = iterator.key();
					break;
				} else {
					count -= iterator.value();
				}
			}
		}
		//should never return default string, since sum of word count over all entries should equal totalWordCount
		return chosenWord;
	}
	
	@Override
	public int size() {
		return this.totalWordCount;
	}
	
	@Override
	public Bigram getBigram() {
		return this.bigram;
	}

	@Override
	public String getId() {
		return this.id;
	}
	
	public int numEntries() {
		return this.words.size();
	}
	
	private void incrementTotal() {
		this.totalWordCount++;
	}
	
	/*
	 * note we need this to propagate thrown IOExceptions, and any thrown IOExceptions 
	 * will be rethrown as UncheckedIOException() via the IOProcedure wrapper
	 */
	boolean forEachEntry(IOProcedure<String> procedure) {
		return this.words.forEachEntry(procedure);
	}

	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((bigram == null) ? 0 : bigram.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!(obj instanceof LargeFollowingWordSet))
			return false;
		LargeFollowingWordSet other = (LargeFollowingWordSet) obj;
		if (bigram == null) {
			if (other.bigram != null)
				return false;
		} else if (!bigram.equals(other.bigram))
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("LargeFollowingWordSet [words=");
		builder.append(words);
		builder.append(", totalWordCount=");
		builder.append(totalWordCount);
		builder.append("]");
		return builder.toString();
	}
	
	@Override
	public String toStringPlain() {
		return words.toString();
	}

	@Override
	public Type getType() {
		return FollowingWordSet.Type.LARGE;
	}

	/*
	 * synchronized because trove collections must be manually synchronized on when iterating
	 */
	@Override
	public synchronized void writeToOutput(FSTObjectOutput out) throws IOException {

		out.writeInt(this.getType().getValue());
		out.writeInt(this.numEntries());
		try {
			this.forEachEntry((word, count) ->
			{
				out.writeUTF(word);
				out.writeInt(count);
				return true;
			});
		} catch (UncheckedIOException ex) {
			//rethrow outside of lambda
			throw ex.getCause();
		}
	}

}
