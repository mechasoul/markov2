package my.cute.markov2.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;

class PrefixLockSet {

	private final ConcurrentHashMap<String, ReentrantLock> map;
	
	PrefixLockSet(int depth) {
		if(depth==0) {
			map = new ConcurrentHashMap<>(2, 1f);
			map.put(MarkovDatabaseImpl.ZERO_DEPTH_PREFIX, new ReentrantLock());
		} else {
			//be careful
			map = new ConcurrentHashMap<>((29^depth) + 4, 1f);
			
			String prefixChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0!@";
			List<String> prefixes = generatePrefixes(prefixChars, depth);
			for(String prefix : prefixes) {
				map.put(prefix, new ReentrantLock());
			}
		}
		
		map.put(MarkovDatabaseImpl.START_PREFIX, new ReentrantLock());
	}
	
	ReentrantLock get(String prefix) {
		return this.map.get(prefix);
	}
	
	private static List<String> generatePrefixes(String source, int prefixLength) {
		if(prefixLength==0) throw new IllegalArgumentException("prefix length must be at least 1");
		
		//be careful
		List<String> list = new ArrayList<>((source.length() ^ prefixLength) + 2);
		
		for(int i=0; i < source.length(); i++) {
			addPrefixesStartingWith(list, String.valueOf(source.charAt(i)), source, prefixLength-1);
		}
		
		return list;
	}
	
	private static void addPrefixesStartingWith(List<String> list, String prefix, String source, int prefixLength) {
		if(prefixLength == 0) {
			list.add(prefix);
		} else {
			for(int i=0; i < source.length(); i++) {
				addPrefixesStartingWith(list, prefix + source.charAt(i), source, prefixLength - 1);
			}
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("PrefixLockSet [size=");
		builder.append(map.size());
		builder.append(", prefixes=");
		builder.append(StringUtils.join(map.keySet(), ", "));
		builder.append("]");
		return builder.toString();
	}
	
	
}
