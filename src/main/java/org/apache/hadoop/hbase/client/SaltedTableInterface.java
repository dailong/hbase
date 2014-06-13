package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public class SaltedTableInterface implements HTableInterface {
  private KeySalter salter;
  private HTableInterface table;

  public SaltedTableInterface(HTableInterface table, KeySalter salter) {
    this.table = table;
    this.salter = salter;
  }

  public Result get(Get get) throws IOException {
    return unSalt(this.table.get(salt(get)));
  }

  public ResultScanner getScanner(Scan scan) throws IOException {
    return getScanner(scan, (byte[][]) null);
  }

  public ResultScanner getScanner(Scan scan, byte[][] salts)
      throws IOException {
    return new SaltedScanner(scan, salts, false);
  }

  public ResultScanner getScanner(Scan scan, byte[][] salts, boolean keepSalt)
      throws IOException {
    return new SaltedScanner(scan, salts, keepSalt);
  }

  public void put(Put put) throws IOException {
    this.table.put(salt(put));
  }

  public void delete(Delete delete) throws IOException {
    this.table.delete(salt(delete));
  }

  private Get salt(Get get) throws IOException {
    if (null == get) {
      return null;
    }
    Get newGet = new Get(this.salter.salt(get.getRow()));
    newGet.setFilter(get.getFilter());
    newGet.setCacheBlocks(get.getCacheBlocks());
    newGet.setMaxVersions(get.getMaxVersions());
    newGet.setTimeRange(get.getTimeRange().getMin(), get.getTimeRange()
        .getMax());
    newGet.getFamilyMap().putAll(get.getFamilyMap());
    return newGet;
  }

  private Delete salt(Delete delete) {
    if (null == delete) {
      return null;
    }
    byte[] newRow = this.salter.salt(delete.getRow());
    Delete newDelete = new Delete(newRow);

    Map<byte[], List<KeyValue>> newMap = salt(delete.getFamilyMap());
    newDelete.getFamilyMap().putAll(newMap);
    return newDelete;
  }

  private Put salt(Put put) {
    if (null == put) {
      return null;
    }
    byte[] newRow = this.salter.salt(put.row);
    Put newPut = new Put(newRow, put.ts);
    Map<byte[], List<KeyValue>> newMap = salt(put.getFamilyMap());
    newPut.getFamilyMap().putAll(newMap);
    newPut.writeToWAL = put.writeToWAL;
    return newPut;
  }

  private Map<byte[], List<KeyValue>> salt(
      Map<byte[], List<KeyValue>> familyMap) {
    if (null == familyMap) {
      return null;
    }
    Map<byte[], List<KeyValue>> result = new HashMap<byte[], List<KeyValue>>();

    for (Map.Entry<byte[], List<KeyValue>> entry : familyMap.entrySet()) {
      List<KeyValue> kvs = entry.getValue();
      if (null != kvs) {
        List<KeyValue> newKvs = new ArrayList<KeyValue>();
        for (int i = 0; i < kvs.size(); i++) {
          newKvs.add(salt(kvs.get(i)));
        }
        result.put(entry.getKey(), newKvs);
      }
    }
    return result;
  }

  private KeyValue salt(KeyValue kv) {
    if (null == kv) {
      return null;
    }
    byte[] newRow = this.salter.salt(kv.getRow());
    return new KeyValue(newRow, 0, newRow.length, kv.getBuffer(),
        kv.getFamilyOffset(), kv.getFamilyLength(), kv.getBuffer(),
        kv.getQualifierOffset(), kv.getQualifierLength(),
        kv.getTimestamp(), KeyValue.Type.codeToType(kv.getType()),
        kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
  }

  private Result unSalt(Result result) {
    if (null == result) {
      return null;
    }
    KeyValue[] results = result.raw();
    if (null == results) {
      return null;
    }
    KeyValue[] newResults = new KeyValue[results.length];

    for (int i = 0; i < results.length; i++) {
      newResults[i] = unSalt(results[i]);
    }
    return new Result(newResults);
  }

  private KeyValue unSalt(KeyValue kv) {
    if (null == kv) {
      return null;
    }
    byte[] newRowKey = this.salter.unSalt(kv.getRow());
    return new KeyValue(newRowKey, 0, newRowKey.length, kv.getBuffer(),
        kv.getFamilyOffset(), kv.getFamilyLength(), kv.getBuffer(),
        kv.getQualifierOffset(), kv.getQualifierLength(),
        kv.getTimestamp(), KeyValue.Type.codeToType(kv.getType()),
        kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
  }

  public HTableInterface getRawTable() {
    return this.table;
  }

  public byte[] getTableName() {
    return this.table.getTableName();
  }

  public Configuration getConfiguration() {
    return this.table.getConfiguration();
  }

  public HTableDescriptor getTableDescriptor() throws IOException {
    return this.table.getTableDescriptor();
  }

  public boolean isAutoFlush() {
    return this.table.isAutoFlush();
  }

  public void flushCommits() throws IOException {
    this.table.flushCommits();
  }

  public void close() throws IOException {
    this.table.close();
  }

  public boolean exists(Get get) throws IOException {
    Get newGet = salt(get);
    return this.table.exists(newGet);
  }

  public Result[] get(List<Get> gets) throws IOException {
    if ((null == gets) || (gets.size() == 0)) {
      return null;
    }
    Result[] result = new Result[gets.size()];
    for (int i = 0; i < gets.size(); i++) {
      Get newGet = salt((Get) gets.get(i));
      result[i] = unSalt(this.table.get(newGet));
    }
    return result;
  }

  public void put(List<Put> puts) throws IOException {
    if ((null == puts) || (puts.size() == 0)) {
      return;
    }
    List<Put> newPuts = new ArrayList<Put>(puts.size());
    for (int i = 0; i < puts.size(); i++) {
      newPuts.add(salt(puts.get(i)));
    }
    this.table.put(newPuts);
  }

  public void delete(List<Delete> deletes) throws IOException {
    if ((null == deletes) || (deletes.size() == 0)) {
      return;
    }
    List<Delete> newDeletes = new ArrayList<Delete>(deletes.size());
    for (int i = 0; i < deletes.size(); i++) {
      newDeletes.add(salt(deletes.get(i)));
    }
    this.table.delete(newDeletes);
  }

  public Result append(Append append) throws IOException {
    Result result = this.table.append(salt(append));
    return unSalt(result);
  }

  private Append salt(Append append) {
    if (null == append) {
      return null;
    }
    byte[] newRow = this.salter.salt(append.getRow());
    Append newAppend = new Append(newRow);

    Map<byte[], List<KeyValue>> newMap = salt(append.getFamilyMap());
    newAppend.getFamilyMap().putAll(newMap);
    return newAppend;
  }

  @SuppressWarnings("deprecation")
  public RowLock lockRow(byte[] row) throws IOException {
    byte[] newRow = this.salter.salt(row);
    return this.table.lockRow(newRow);
  }

  @SuppressWarnings("deprecation")
  public void unlockRow(RowLock rl) throws IOException {
    this.table.unlockRow(rl);
  }

  @SuppressWarnings("deprecation")
  public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
    byte[] newRow = this.salter.salt(row);
    Result result = this.table.getRowOrBefore(newRow, family);
    return unSalt(result);
  }

  public ResultScanner getScanner(byte[] family) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(family);
    return getScanner(scan);
  }

  public ResultScanner getScanner(byte[] family, byte[] qualifier)
      throws IOException {
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return getScanner(scan);
  }

  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Put put) throws IOException {
    byte[] newRow = this.salter.salt(row);
    Put newPut = salt(put);
    return this.table.checkAndPut(newRow, family, qualifier, value, newPut);
  }

  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Delete delete) throws IOException {
    byte[] newRow = this.salter.salt(row);
    Delete newDelete = salt(delete);
    return this.table.checkAndDelete(newRow, family, qualifier, value,
      newDelete);
  }

  public void mutateRow(RowMutations rm) throws IOException {
    throw new UnsupportedOperationException(
        "Please use getRawTable to get underlying table");
  }

  public long incrementColumnValue(byte[] row, byte[] family,
      byte[] qualifier, long amount) throws IOException {
    byte[] newRow = this.salter.salt(row);
    return this.table.incrementColumnValue(newRow, family, qualifier,
      amount);
  }

  public long incrementColumnValue(byte[] row, byte[] family,
      byte[] qualifier, long amount, boolean writeToWAL)
      throws IOException {
    byte[] newRow = this.salter.salt(row);
    return this.table.incrementColumnValue(newRow, family, qualifier,
      amount, writeToWAL);
  }

  public Result increment(Increment increment) throws IOException {
    throw new UnsupportedOperationException(
        "Please use getRawTable to get underlying table");
  }

  public void batch(List<? extends Row> actions, Object[] results)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException(
        "Please use getRawTable to get underlying table");
  }

  public Object[] batch(List<? extends Row> actions) throws IOException,
      InterruptedException {
    throw new UnsupportedOperationException(
        "Please use getRawTable to get underlying table");
  }

  public <T extends CoprocessorProtocol> T coprocessorProxy(
      Class<T> protocol, byte[] row) {
    throw new UnsupportedOperationException(
        "Please use getRawTable to get underlying table");
  }

  public <T extends CoprocessorProtocol, R> Map<byte[], R> coprocessorExec(
      Class<T> protocol, byte[] startKey, byte[] endKey,
      Batch.Call<T, R> callable) throws IOException, Throwable {
    throw new UnsupportedOperationException(
        "Please use getRawTable to get underlying table");
  }

  public <T extends CoprocessorProtocol, R> void coprocessorExec(
      Class<T> protocol, byte[] startKey, byte[] endKey,
      Batch.Call<T, R> callable, Batch.Callback<R> callback)
      throws IOException, Throwable {
    throw new UnsupportedOperationException(
        "Please use getRawTable to get underlying table");
  }

  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
    this.table.setAutoFlush(autoFlush, clearBufferOnFail);
  }

  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    this.table.setWriteBufferSize(writeBufferSize);
  }

  public long getWriteBufferSize() {
    return this.table.getWriteBufferSize();
  }

  private class SaltedScanner implements ResultScanner {
    private MergeSortScanner scanner;
    private boolean keepSalt;

    public SaltedScanner(Scan scan, byte[][] salts, boolean keepSalt)
        throws IOException {
      Scan[] scans = salt(scan, salts);
      this.keepSalt = keepSalt;
      this.scanner = new MergeSortScanner(scans,
          SaltedTableInterface.this.table,
          SaltedTableInterface.this.salter.getSaltLength());
    }

    public Iterator<Result> iterator() {
      return new Iterator<Result>() {
        public boolean hasNext() {
          return SaltedTableInterface.SaltedScanner.this.scanner
              .iterator().hasNext();
        }

        public Result next() {
          if (SaltedTableInterface.SaltedScanner.this.keepSalt) {
            return (Result) SaltedTableInterface.SaltedScanner.this.scanner
                .iterator().next();
          }

          return SaltedTableInterface.this
              .unSalt((Result) SaltedTableInterface.SaltedScanner.this.scanner
                  .iterator().next());
        }

        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    public Result next() throws IOException {
      if (this.keepSalt) {
        return this.scanner.next();
      }

      return SaltedTableInterface.this.unSalt(this.scanner.next());
    }

    public Result[] next(int nbRows) throws IOException {
      ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
      for (int i = 0; i < nbRows; i++) {
        Result next = next();
        if (next == null) break;
        resultSets.add(next);
      }

      return (Result[]) resultSets.toArray(new Result[resultSets.size()]);
    }

    public void close() {
      this.scanner.close();
    }

    private Scan[] salt(Scan scan, byte[][] salts) throws IOException {
      byte[][] splits = (byte[][]) null;
      if (null != salts) {
        splits = salts;
      } else {
        splits = SaltedTableInterface.this.salter.getAllSalts();
      }
      Scan[] scans = new Scan[splits.length];
      byte[] start = scan.getStartRow();
      byte[] end = scan.getStopRow();

      for (int i = 0; i < splits.length; i++) {
        scans[i] = new Scan(scan);
        scans[i].setStartRow(concat(splits[i], start));
        if (end.length == 0) {
          scans[i].setStopRow(i == splits.length - 1 ? HConstants.EMPTY_END_ROW
              : splits[(i + 1)]);
        } else {
          scans[i].setStopRow(concat(splits[i], end));
        }
      }
      return scans;
    }

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

  @Override
  public void setAutoFlush(boolean autoFlush) {
    table.setAutoFlush(autoFlush);
  }
}