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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ProgressableCancellable;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Classes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class BatchExec extends Invocation {
  private byte[] regionName;
  private Class<? extends CoprocessorProtocol> protocol;
  private String protocolName;

  public BatchExec() {
  }

  public BatchExec(Configuration configuration, byte[] regionName,
      Class<? extends CoprocessorProtocol> protocol, Method method,
      Object[] parameters) {
    super(method, protocol, parameters);
    this.conf = configuration;
    this.regionName = regionName;
    this.protocol = protocol;
    this.protocolName = protocol.getName();
  }

  public String getProtocolName() {
    return this.protocolName;
  }

  public Class<? extends CoprocessorProtocol> getProtocol() {
    return this.protocol;
  }

  public byte[] getRegionName() {
    return this.regionName;
  }

  public ProgressableCancellable[] getProgressableCancellableParameters() {
    List<ProgressableCancellable> params = new ArrayList<ProgressableCancellable>();

    for (Object parameter : this.parameters) {
      if ((parameter instanceof ProgressableCancellable)) {
        params.add((ProgressableCancellable) parameter);
      }
    }
    return (ProgressableCancellable[]) params
        .toArray(new ProgressableCancellable[params.size()]);
  }

  public void write(DataOutput out) throws IOException {
    out.writeUTF(this.methodName);
    out.writeInt(this.parameterClasses.length);
    for (int i = 0; i < this.parameterClasses.length; i++) {
      HbaseObjectWritable.writeObject(out, this.parameters[i],
              this.parameters[i] != null ? this.parameters[i].getClass()
                      : this.parameterClasses[i], this.conf
      );

      out.writeUTF(this.parameterClasses[i].getName());
    }

    Bytes.writeByteArray(out, this.regionName);
    out.writeUTF(this.protocol.getName());
  }

  public void readFields(DataInput in) throws IOException {
    this.methodName = in.readUTF();
    this.parameters = new Object[in.readInt()];
    this.parameterClasses = new Class[this.parameters.length];
    HbaseObjectWritable objectWritable = new HbaseObjectWritable();
    for (int i = 0; i < this.parameters.length; i++) {
      this.parameters[i] = HbaseObjectWritable.readObject(in,
              objectWritable, this.conf);

      String parameterClassName = in.readUTF();
      try {
        this.parameterClasses[i] = Classes
            .extendedForName(parameterClassName);
      } catch (ClassNotFoundException e) {
        throw new IOException("Couldn't find class: "
            + parameterClassName);
      }
    }

    this.regionName = Bytes.readByteArray(in);
    this.protocolName = in.readUTF();
  }
}