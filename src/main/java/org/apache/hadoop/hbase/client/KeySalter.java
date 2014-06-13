package org.apache.hadoop.hbase.client;

public abstract interface KeySalter {
  public abstract int getSaltLength();

  public abstract byte[][] getAllSalts();

  public abstract byte[] salt(byte[] paramArrayOfByte);

  public abstract byte[] unSalt(byte[] paramArrayOfByte);
}