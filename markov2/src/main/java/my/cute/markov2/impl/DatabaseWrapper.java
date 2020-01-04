//will be more complex in fws-B due to different database types
//serializer must be rewritten

package my.cute.markov2.impl;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.nustaq.serialization.FSTBasicObjectSerializer;
import org.nustaq.serialization.FSTClazzInfo;
import org.nustaq.serialization.FSTClazzInfo.FSTFieldInfo;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;
import org.nustaq.serialization.annotations.Flat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import gnu.trove.TCollections;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

@Flat
class DatabaseWrapper implements Serializable {

	@SuppressWarnings("unused")
	private static final Logger logger = LoggerFactory.getLogger(DatabaseWrapper.class);
	private static final long serialVersionUID = 1L;

	static class Serializer extends FSTBasicObjectSerializer {

		@Override
		public void writeObject(FSTObjectOutput out, Object toWrite, FSTClazzInfo clzInfo, FSTFieldInfo referencedBy,
				int streamPosition) throws IOException {
			
			DatabaseWrapper db = (DatabaseWrapper) toWrite;
			out.writeInt(db.size());
			out.writeUTF(db.getKey());
			out.writeUTF(db.getId());
			for(Map.Entry<Bigram, FollowingWordSet> next : db.entrySet()) {
				out.writeObject(next.getKey(), Bigram.class);
				next.getValue().writeToOutput(out);
			}
		}
		
		@Override
	    public void readObject(FSTObjectInput in, Object toRead, FSTClazzInfo clzInfo, FSTClazzInfo.FSTFieldInfo referencedBy)
	    {
	    }
		
		@Override
		public Object instantiate(@SuppressWarnings("rawtypes") Class objectClass, FSTObjectInput in, FSTClazzInfo serializationInfo, FSTClazzInfo.FSTFieldInfo referencee, int streamPosition) throws Exception 
		{
			
			int dbSize = in.readInt();
			String key = MyStringPool.INSTANCE.intern(in.readUTF());
			String id = MyStringPool.INSTANCE.intern(in.readUTF());
			ConcurrentMap<Bigram, FollowingWordSet> db = new ConcurrentHashMap<Bigram, FollowingWordSet>(dbSize * 4 / 3, 0.8f);
			for(int i=0; i < dbSize; i++) {
				Bigram bigram = (Bigram) in.readObject(Bigram.class);
				
				FollowingWordSet fws;
				FollowingWordSet.Type type = FollowingWordSet.Type.fromInt(in.readInt());
				if(type == FollowingWordSet.Type.SMALL) {
					int listSize = in.readInt();
					List<String> list = Collections.synchronizedList(new ArrayList<String>(listSize));
					for(int j=0; j < listSize; j++) {
						list.add(MyStringPool.INSTANCE.intern(in.readUTF()));
					}
					fws = new SmallFollowingWordSet(list, bigram, id);
				} else if(type == FollowingWordSet.Type.TINY) {
					int listSize = in.readInt();
					ImmutableList.Builder<String> builder = ImmutableList.<String>builderWithExpectedSize(listSize);
					for(int j=0; j < listSize; j++) {
						builder.add(MyStringPool.INSTANCE.intern(in.readUTF()));
					}
					fws = TinyFollowingWordSet.of(builder.build());
				} else {
					//type == FollowingWordSet.Type.LARGE
					int mapSize = in.readInt();
					TObjectIntMap<String> map = TCollections.synchronizedMap(new TObjectIntHashMap<String>(mapSize * 5 / 4, 0.8f));
					int totalWordCount=0;
					for(int j=0; j < mapSize; j++) {
						String word = MyStringPool.INSTANCE.intern(in.readUTF());
						int wordCount = in.readInt();
						totalWordCount += wordCount;
						map.put(word, wordCount);
					}
					fws = new LargeFollowingWordSet(map, totalWordCount, bigram, id);
				}
				db.put(bigram, fws);
			}
			DatabaseWrapper object = new DatabaseWrapper(db, key, id);
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
	
	private final String key;
	private final String parentDatabaseId;
	private final ConcurrentMap<Bigram, FollowingWordSet> database;
	
//	DatabaseWrapper() {
//		this.database = new ConcurrentHashMap<Bigram, FollowingWordSet>(3);
//		//danger
//		this.prefix = null;
//		this.parentDatabaseId = null;
//	}
	
	DatabaseWrapper(String key, String id) {
		this.database = new ConcurrentHashMap<Bigram, FollowingWordSet>(1);
		this.key = key;
		this.parentDatabaseId = id;
	}
	
	DatabaseWrapper(ConcurrentMap<Bigram, FollowingWordSet> map, String key, String id) {
		this.database = map;
		this.key = key;
		this.parentDatabaseId = id;
	}

	public FollowingWordSet get(Bigram bigram) {
		return this.database.get(bigram);
	}
	
	public FollowingWordSet put(Bigram bigram, FollowingWordSet words) {
		return this.database.put(bigram, words);
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
	
	boolean remove(Bigram bigram) {
		return this.database.remove(bigram) != null;
	}

	public String getKey() {
		return this.key;
	}

	public String getId() {
		return this.parentDatabaseId;
	}
	
	/*
	 * re: hashCode() and equals(), two shard db wrappers should be considered equal iff they represent the same
	 * shard in the same markovdb, which is reflected here
	 * i think this makes more sense than db wrapper equality depending on its contents, and it also
	 * means a DatabaseWrapper's hashcode and equality will not change after creation
	 * its slightly abstract and possibly unintuitive but again i think it does make sense
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((parentDatabaseId == null) ? 0 : parentDatabaseId.hashCode());
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!(obj instanceof DatabaseWrapper))
			return false;
		DatabaseWrapper other = (DatabaseWrapper) obj;
		if (parentDatabaseId == null) {
			if (other.parentDatabaseId != null)
				return false;
		} else if (!parentDatabaseId.equals(other.parentDatabaseId))
			return false;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("DatabaseWrapper [key=");
		builder.append(key);
		builder.append(", parentDatabaseId=");
		builder.append(parentDatabaseId);
		builder.append("]");
		return builder.toString();
	}

	
}
