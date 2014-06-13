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

public abstract class AbstractDoubleColumnInterpreter implements
    ColumnInterpreter<Double, Double> {
  public Double add(Double d1, Double d2) {
    if (((d1 == null ? 1 : 0) ^ (d2 == null ? 1 : 0)) != 0) {
      return d1 == null ? d2 : d1;
    }
    if (d1 == null) {
      return null;
    }
    return Double.valueOf(d1.doubleValue() + d2.doubleValue());
  }

  public Double getMaxValue() {
    return Double.valueOf(1.7976931348623157E+308D);
  }

  public Double getMinValue() {
    return Double.valueOf(4.9E-324D);
  }

  public Double multiply(Double d1, Double d2) {
    return (d1 == null) || (d2 == null) ? null : Double.valueOf(d1
        .doubleValue() * d2.doubleValue());
  }

  public Double increment(Double o) {
    return null;
  }

  public Double castToReturnType(Double o) {
    return o;
  }

  public int compare(Double d1, Double d2) {
    if (((d1 == null ? 1 : 0) ^ (d2 == null ? 1 : 0)) != 0) {
      return d1 == null ? -1 : 1;
    }
    if (d1 == null) {
      return 0;
    }
    return d1.compareTo(d2);
  }

  public double divideForAvg(Double d, Long l) {
    return (l == null) || (d == null) ? (0.0D / 0.0D) : d.doubleValue()
        / l.doubleValue();
  }

  public void readFields(DataInput arg0) throws IOException {
  }

  public void write(DataOutput arg0) throws IOException {
  }
}