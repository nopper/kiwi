package com.kv.indexer;

class DB
{
  public native int open(String path);
  public native int close();
  public native int add(byte[] key, int klen, byte[] value, int vlen);
  public native int get(byte[] key, int klen, byte[] value);
  public native int remove(byte[] key, int klen);
  static {
    System.loadLibrary("indexer_java");
  }
}
