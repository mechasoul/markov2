package my.cute.markov2.impl;

import java.io.IOException;
import java.io.Serializable;

import org.nustaq.serialization.FSTBasicObjectSerializer;
import org.nustaq.serialization.FSTClazzInfo;
import org.nustaq.serialization.FSTClazzInfo.FSTFieldInfo;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;
import org.nustaq.serialization.annotations.Flat;

@Flat
public class Bigram implements Serializable {
	
	static class Serializer extends FSTBasicObjectSerializer {

		@Override
		public void writeObject(FSTObjectOutput out, Object toWrite, FSTClazzInfo clzInfo, FSTFieldInfo referencedBy,
				int streamPosition) throws IOException {
			Bigram bigram = (Bigram) toWrite;
			out.writeUTF(bigram.getWord1());
			out.writeUTF(bigram.getWord2());
		}
		
		@Override
	    public void readObject(FSTObjectInput in, Object toRead, FSTClazzInfo clzInfo, FSTClazzInfo.FSTFieldInfo referencedBy)
	    {
	    }
		
		@Override
		public Object instantiate(@SuppressWarnings("rawtypes") Class objectClass, FSTObjectInput in, FSTClazzInfo serializationInfo, FSTClazzInfo.FSTFieldInfo referencee, int streamPosition) throws IOException 
		{
			String word1 = MyStringPool.INSTANCE.intern(in.readUTF());
			String word2 = MyStringPool.INSTANCE.intern(in.readUTF());
			Object bigram = new Bigram(word1, word2);
			in.registerObject(bigram, streamPosition, serializationInfo, referencee);
			return bigram;
		}
	}

	private static final long serialVersionUID = 1L;
	private final String word1;
	private final String word2;
	private int hash = 0;
	
	public Bigram() {
		word1 = MyStringPool.INSTANCE.intern("");
		word2 = MyStringPool.INSTANCE.intern("");
	}
	
	public Bigram(String w1, String w2) {
		word1 = MyStringPool.INSTANCE.intern(w1);
		word2 = MyStringPool.INSTANCE.intern(w2);
	}
	
	public String getWord1() {
		return this.word1;
	}
	
	public String getWord2() {
		return this.word2;
	}
	
	
	
	@Override
	public int hashCode() {
		//recalculate every time hash is 0, but shouldn't be too much of an issue
		if(this.hash==0) {
			final int prime = 31;
			int h = 1;
			h = prime * h + ((word1 == null) ? 0 : word1.hashCode());
			h = prime * h + ((word2 == null) ? 0 : word2.hashCode());
			this.hash = h;
		}
		return this.hash;
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
