package my.cute.markov2.impl;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.nustaq.serialization.FSTObjectOutput;
import org.nustaq.serialization.annotations.Flat;

@Flat
class SmallFollowingWordSet implements FollowingWordSet, Serializable, Iterable<String> {

//	static class Serializer extends FSTBasicObjectSerializer {
//
//		@Override
//		public void writeObject(FSTObjectOutput out, Object toWrite, FSTClazzInfo clzInfo, FSTFieldInfo referencedBy,
//				int streamPosition) throws IOException {
//			
//			SmallFollowingWordSet fws = (SmallFollowingWordSet) toWrite;
//			out.writeInt(fws.size());
//			synchronized(fws) {
//				for(String word : fws) {
//					out.writeUTF(word);
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
//			SmallFollowingWordSet obj = new SmallFollowingWordSet(size);
//			for(int i=0; i < size; i++) {
//				obj.addWord(MyStringPool.INSTANCE.intern(in.readUTF()));
//			}
//			in.registerObject(obj, streamPosition, serializationInfo, referencee);
//			return obj;
//		}
//	}
	
	private static final long serialVersionUID = 1L;
	private final List<String> words;
	/*
	 * the bigram this fws corresponds to
	 */
	private final Bigram bigram;
	/*
	 * the id for the server this fws (and its corresponding bigram) belong to
	 */
	private final String id;
	
//	SmallFollowingWordSet() {
//		this.words = Collections.synchronizedList(new ArrayList<String>(1));
//	}
//	
	SmallFollowingWordSet(String firstWord, Bigram bigram, String id) {
		this.words = Collections.synchronizedList(new ArrayList<String>(1));
		this.addWord(firstWord);
		this.bigram = bigram;
		this.id = id;
	}
//	
//	SmallFollowingWordSet(int size) {
//		this.words = Collections.synchronizedList(new ArrayList<String>(size));
//	}
	
	SmallFollowingWordSet(List<String> list, Bigram bigram, String id) {
		this.words = list;
		this.bigram = bigram;
		this.id = id;
	}
	
	@Override
	public void addWord(String word) {
		this.words.add(word);
	}

	@Override
	public String getRandomWeightedWord() {
		return this.words.get(RANDOM.nextInt(this.words.size()));
	}

	@Override
	public int size() {
		return this.words.size();
	}
	
	/*
	 * O(n) for this implementation of FollowingWordSet. use sparingly
	 */
	@Override
	public boolean contains(String followingWord) {
		synchronized(this.words) {
			return this.words.contains(followingWord);
		}
	}
	
	@Override
	public boolean contains(String followingWord, int count) {
		synchronized(this.words) {
			return (Collections.frequency(this.words, followingWord) >= count);
		}
	}

	/*
	 * O(n) for this implementation of FollowingWordSet. use sparingly
	 */
	@Override
	public boolean remove(String followingWord) {
		synchronized(this.words) {
			return this.words.remove(followingWord);
		}
	}
	
	@Override
	public boolean isEmpty() {
		return this.words.isEmpty();
	}
	
	@Override
	public Bigram getBigram() {
		return this.bigram;
	}

	@Override
	public String getId() {
		return this.id;
	}

	/*
	 * must synchronize manually on this when iterating
	 */
	@Override
	public Iterator<String> iterator() {
		return this.words.iterator();
	}

	/*
	 * for synchronizing on
	 */
	List<String> getRawWords() {
		return this.words;
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
		if (!(obj instanceof SmallFollowingWordSet))
			return false;
		SmallFollowingWordSet other = (SmallFollowingWordSet) obj;
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
		builder.append("SmallFollowingWordSet [words=");
		builder.append(words);
		builder.append("]");
		return builder.toString();
	}
	
	@Override
	public String toStringPlain() {
		return words.toString();
	}

	@Override
	public Type getType() {
		return FollowingWordSet.Type.SMALL;
	}

	@Override
	public void writeToOutput(FSTObjectOutput out) throws IOException {
		
		out.writeInt(this.getType().getValue());
		out.writeInt(this.size());
		synchronized(this.words) {
			for(String word : this) {
				out.writeUTF(word);
			}
		}
	}

	
}
