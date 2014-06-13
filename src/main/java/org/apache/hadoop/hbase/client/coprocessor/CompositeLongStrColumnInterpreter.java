/*
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client.coprocessor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

public class CompositeLongStrColumnInterpreter extends
    AbstractLongColumnInterpreter {
  private String delimiter = ",";
  private int index = 0;
  private Pattern pattern = null;

  public CompositeLongStrColumnInterpreter() {
  }

  public CompositeLongStrColumnInterpreter(String delimiter, int index) {
    this.delimiter = delimiter;
    this.index = index;
  }

  public Long getValue(byte[] colFamily, byte[] colQualifier, KeyValue kv) throws IOException {
    if (kv == null) {
      return null;
    }
    String val = Bytes.toString(kv.getBuffer(), kv.getValueOffset(),
      kv.getValueLength());

    if (val == null) {
      return null;
    }
    if (this.index < 0) {
      return null;
    }
    if (this.pattern == null) {
      this.pattern = Pattern.compile(this.delimiter);
    }
    String[] parts = this.pattern.split(val, this.index + 2);
    if (parts.length <= this.index) return null;
    try {
      val = parts[this.index];
      return Long.valueOf(val);
    } catch (NumberFormatException e) {
    }
    return null;
  }

  public String getDelimiter() {
    return this.delimiter;
  }

  public int getIndex() {
    return this.index;
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.delimiter = in.readUTF();
    this.index = in.readInt();
    this.pattern = null;
  }

  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(this.delimiter);
    out.writeInt(this.index);
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj instanceof CompositeLongStrColumnInterpreter)) {
      CompositeLongStrColumnInterpreter another = (CompositeLongStrColumnInterpreter) obj;
      return (this.delimiter.equals(another.getDelimiter()))
          && (this.index == another.getIndex());
    }

    return false;
  }
}