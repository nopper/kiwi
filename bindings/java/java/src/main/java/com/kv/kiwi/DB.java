package com.kv.kiwi;

import java.nio.charset.Charset;

public class DB
{
  public native int open(String path);
  public native int close();
  public native int add(byte[] key, int klen, byte[] value, int vlen);
  public native byte[] get(byte[] key, int klen);
  public native int remove(byte[] key, int klen);
  static {
  	System.loadLibrary("jniKiwiNative");
  }
  public static void main(String[] args) {
  	System.out.println("Successfully loaded");
  	DB db = new DB("/tmp");
  	byte[] k = new byte[] {'t', 'e', 's', 't'};
  	byte[] v = new byte[] {'t', 'o', 's', 't'};
  	if (db.add(k, k.length, v, v.length) == 1)
  		System.out.println("Inserted");
  	if (db.get("test").equals("tost"))
  		System.out.println("Retrieved");
  	if (db.remove("test"))
  		System.out.println("Removed");
  	db.shutdown();
  }
  
  public DB(String path) {
	  open(path);
  }
  
  public boolean add(String key, String value) {
	  byte[] karr = key.getBytes(Charset.forName("UTF-8"));
	  byte[] varr = key.getBytes(Charset.forName("UTF-8"));
	  return add(karr, karr.length, varr, varr.length) == 1;
  }
  
  public String get(String key) {
	  byte[] input = get(key.getBytes(), key.getBytes().length);
	  return new String(input);
  }
  
  public boolean remove(String key) {
	  return remove(key.getBytes(), key.getBytes().length) == 1;
  }
  
  public void shutdown() {
	  close();
  }
}
