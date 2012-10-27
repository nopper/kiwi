package com.kv.kiwi;

public class DBIterator {
	private DB database;
	private long iterator;
	
	public DBIterator(DB db, byte[] key) {
		database = db;
		iterator = database.iterator_new();
		database.iterator_seek(iterator, key);
	}
	
	public boolean isValid() {
		return database.iterator_valid(iterator);
	}
	
	public void next() {
		database.iterator_next(iterator);
	}
	
	public byte[] rawKey() {
		return database.iterator_key(iterator);
	}
	
	public byte[] rawValue() {
		return database.iterator_value(iterator);
	}
	
	public String key() {
		byte[] k = database.iterator_key(iterator);
		
		if (k != null && k.length > 0)
			return new String(k);
		
		return "";
	}
	
	public String value() {
		byte[] k = database.iterator_value(iterator);
		
		if (k != null && k.length > 0)
			return new String(k);
		
		return "";
	}
	
	public void destroy() {
		database.iterator_free(iterator);
	}
}
