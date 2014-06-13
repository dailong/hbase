package org.apache.hadoop.hbase.client;

public abstract class NBytePrefixKeySalter implements KeySalter {
  protected int prefixLength;

  public NBytePrefixKeySalter(int prefixLength) {
    this.prefixLength = prefixLength;
  }

  public int getSaltLength() {
    return this.prefixLength;
  }

  public byte[] unSalt(byte[] row) {
    byte[] newRow = new byte[row.length - this.prefixLength];
    System.arraycopy(row, this.prefixLength, newRow, 0, newRow.length);
    return newRow;
  }

  public byte[] salt(byte[] key) {
    return concat(hash(key), key);
  }

  protected abstract byte[] hash(byte[] paramArrayOfByte);

  private byte[] concat(byte[] prefix, byte[] row) {
    if ((null == prefix) || (prefix.length == 0)) {
      return row;
    }
    if ((null == row) || (row.length == 0)) {
      return prefix;
    }
    byte[] newRow = new byte[row.length + prefix.length];
    if (row.length != 0) {
      System.arraycopy(row, 0, newRow, prefix.length, row.length);
    }
    if (prefix.length != 0) {
      System.arraycopy(prefix, 0, newRow, 0, prefix.length);
    }
    return newRow;
  }
}