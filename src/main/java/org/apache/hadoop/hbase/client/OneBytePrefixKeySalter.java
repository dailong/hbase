package org.apache.hadoop.hbase.client;

import java.util.Arrays;
import org.apache.hadoop.hbase.util.Bytes;

public class OneBytePrefixKeySalter extends NBytePrefixKeySalter {
  private static final int ONE_BYTE = 1;
  private int slots;

  public OneBytePrefixKeySalter() {
    this(256);
  }

  public OneBytePrefixKeySalter(int limit) {
    super(ONE_BYTE);
    this.slots = limit;
  }

  protected byte[] hash(byte[] key) {
    byte[] result = new byte[1];
    result[0] = 0;
    int hash = 1;
    if ((key == null) || (key.length == 0)) {
      return result;
    }
    for (int i = 0; i < key.length; i++) {
      hash = 31 * hash + key[i];
    }
    hash &= 2147483647;
    result[0] = (byte) (hash % this.slots);
    return result;
  }

  public byte[][] getAllSalts() {
    byte[][] salts = new byte[this.slots][];
    for (int i = 0; i < salts.length; i++) {
      salts[i] = new byte[] { (byte) i };
    }
    Arrays.sort(salts, Bytes.BYTES_RAWCOMPARATOR);
    return salts;
  }
}