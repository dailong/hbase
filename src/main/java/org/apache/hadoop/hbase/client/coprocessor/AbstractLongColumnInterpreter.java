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
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;

public abstract class AbstractLongColumnInterpreter implements
    ColumnInterpreter<Long, Long> {
  public Long add(Long l1, Long l2) {
    if (((l1 == null ? 1 : 0) ^ (l2 == null ? 1 : 0)) != 0) {
      return l1 == null ? l2 : l1;
    }
    if (l1 == null) {
      return null;
    }
    return Long.valueOf(l1.longValue() + l2.longValue());
  }

  public int compare(Long l1, Long l2) {
    if (((l1 == null ? 1 : 0) ^ (l2 == null ? 1 : 0)) != 0) {
      return l1 == null ? -1 : 1;
    }
    if (l1 == null) {
      return 0;
    }
    return l1.compareTo(l2);
  }

  public Long getMaxValue() {
    return Long.valueOf(9223372036854775807L);
  }

  public Long increment(Long o) {
    return o == null ? null : Long.valueOf(o.longValue() + 1L);
  }

  public Long multiply(Long l1, Long l2) {
    return (l1 == null) || (l2 == null) ? null : Long.valueOf(l1.longValue()
        * l2.longValue());
  }

  public Long getMinValue() {
    return Long.valueOf(-9223372036854775808L);
  }

  public void readFields(DataInput arg0) throws IOException {
  }

  public void write(DataOutput arg0) throws IOException {
  }

  public double divideForAvg(Long l1, Long l2) {
    return (l2 == null) || (l1 == null) ? (0.0D / 0.0D) : l1.doubleValue()
        / l2.doubleValue();
  }

  public Long castToReturnType(Long o) {
    return o;
  }
}