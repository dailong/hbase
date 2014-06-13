/*
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.coprocessor.batch;

import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BatchExecResult implements Writable {
  private List<byte[]> regionNames;
  private Object value;

  public BatchExecResult() {
  }

  public BatchExecResult(List<byte[]> regions, Object value) {
    this.regionNames = regions;
    this.value = value;
  }

  public List<byte[]> getRegionNames() {
    return this.regionNames;
  }

  public Object getValue() {
    return this.value;
  }

  public void write(DataOutput out) throws IOException {
    int count = this.regionNames.size();
    out.writeInt(count);
    for (int i = 0; i < count; i++) {
      Bytes.writeByteArray(out, (byte[]) this.regionNames.get(i));
    }
    HbaseObjectWritable.writeObject(out, this.value,
            this.value != null ? this.value.getClass() : Writable.class,
            null);
  }

  public void readFields(DataInput in) throws IOException {
    int count = in.readInt();
    this.regionNames = new ArrayList<byte[]>();
    for (int i = 0; i < count; i++) {
      this.regionNames.add(Bytes.readByteArray(in));
    }
    this.value = HbaseObjectWritable.readObject(in, null);
  }
}