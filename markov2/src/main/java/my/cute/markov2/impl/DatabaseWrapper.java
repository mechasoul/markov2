//will be more complex in fws-B due to different database types
//serializer must be rewritten

package my.cute.markov2.impl;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.nustaq.serialization.FSTBasicObjectSerializer;
import org.nustaq.serialization.FSTClazzInfo;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTClazzInfo.FSTFieldInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.nustaq.serialization.FSTObjectOutput;

class DatabaseWrapper implements Serializable {

	@SuppressWarnings("unused")
	private static final Logger logger = LoggerFactory.getLogger(DatabaseWrapper.class);
	private static final long serialVersionUID = 1L;

	static class Serializer extends FSTBasicObjectSerializer {

		@Override
		public void writeObject(FSTObjectOutput out, Object toWrite, FSTClazzInfo clzInfo, FSTFieldInfo referencedBy,
				int streamPosition) throws IOException {
			
			DatabaseWrapper db = (DatabaseWrapper) toWrite;
//			out.writeInt(db.entrySet().size());
//			for (Map.Entry<Bigram, List<String>> next : db.entrySet()) {
//				Bigram bigram = next.getKey();
//				out.writeObject(bigram, Bigram.class);
//				int listSize = next.getValue().size();
//				out.writeInt(listSize);
//				for(int i=0; i < listSize; i++) {
//					out.writeUTF(next.getValue().get(i));
//				}
//			}
			
		}
		
		@Override
	    public void readObject(FSTObjectInput in, Object toRead, FSTClazzInfo clzInfo, FSTClazzInfo.FSTFieldInfo referencedBy)
	    {
	    }
		
		@Override
		public Object instantiate(@SuppressWarnings("rawtypes") Class objectClass, FSTObjectInput in, FSTClazzInfo serializationInfo, FSTClazzInfo.FSTFieldInfo referencee, int streamPosition) throws Exception 
		{
			int size = in.readInt();
//			ConcurrentMap<Bigram, List<String>> db = new ConcurrentHashMap<Bigram, List<String>>(size * 4 / 3);
//			for(int i=0; i < size; i++) {
//				Bigram bigram = (Bigram) in.readObject(Bigram.class);
//				int listSize = in.readInt();
//				List<String> list = Collections.synchronizedList(new ArrayList<>(listSize));
//				for(int j=0; j < listSize; j++) {
//					list.add(MyStringPool.INSTANCE.intern(in.readUTF()));
//				}
//				db.put(bigram, list);
//			}
			
			
			
			DatabaseWrapper object = new DatabaseWrapper(db);
			in.registerObject(object, streamPosition, serializationInfo, referencee);
			return object;
		}
	}
	
	/*
	 * TODO continue fixing this stuff i guess
	 * the custom serializer seems to work properly but needs to be tested in multithread environment
	 * seems fine in multithreaded environment
	 * cant tell if strong or weak references in string pool are better? need test under heap load
	 * kinda want to test native intern() again? if it was blocking on io or something maybw its better now
	 * maybe test skipping the custom bigram serializer and just integrate it into db serializing directly
	 * this is close to being as good as its gonna get i guess
	 * need to test 0 db size for minimal memory
	 * see how small of a heap it fits into
	 * also interested in trying like, immutable arraylists for followingwordsets 
	 * if we intern them theres potential for a lot of memory saving i think?
	 * eg theres probably a ton of fws that are just {<_end>}, and theyre all duuplicated
	 * prob leave that for after though, i want to test fws-B first
	 * & we'll have to merge guava interning + fst serializing into master too
	 */
	
	private final ConcurrentMap<Bigram, FollowingWordSet> database;
	
	DatabaseWrapper() {
		this.database = new ConcurrentHashMap<Bigram, FollowingWordSet>(3);
	}
	
	DatabaseWrapper(ConcurrentMap<Bigram, FollowingWordSet> map) {
		this.database = map;
	}

	public FollowingWordSet get(Bigram key) {
		return this.database.get(key);
	}

	public Iterator<Map.Entry<Bigram, FollowingWordSet>> iterator() {
		return this.database.entrySet().iterator();
	}

	public FollowingWordSet put(Bigram key, FollowingWordSet value) {
		return this.database.put(key, value);
	}

	public int size() {
		return this.database.size();
	}

	public FollowingWordSet putIfAbsent(Bigram key, FollowingWordSet value) {
		return this.database.putIfAbsent(key, value);
	}

	public Set<Map.Entry<Bigram, FollowingWordSet>> entrySet() {
		return this.database.entrySet();
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("DatabaseWrapper [database=");
		builder.append(database);
		builder.append("]");
		return builder.toString();
	}
}
