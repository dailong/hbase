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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class BatchExecRPCInvoker<R> implements InvocationHandler {
  private Configuration conf;
  private final HConnection connection;
  private final ServerName serverName;
  private Class<? extends CoprocessorProtocol> protocol;
  private BatchExecCall.ServerCallback<R> serverCallback;
  private List<BatchExec> execList;
  private byte[] regionName;

  public BatchExecRPCInvoker(Configuration conf, HConnection connection,
      ServerName serverName,
      Class<? extends CoprocessorProtocol> protocol,
      BatchExecCall.ServerCallback<R> serverCallback) {
    this.conf = conf;
    this.connection = connection;
    this.serverName = serverName;
    this.protocol = protocol;
    this.serverCallback = serverCallback;
    this.execList = new ArrayList<BatchExec>();
  }

  public void setRegionName(byte[] regionName) {
    this.regionName = regionName;
  }

  public Object invoke(Object instance, Method method, Object[] args)
      throws Throwable {
    if (this.regionName != null) {
      this.execList.add(new BatchExec(this.conf, this.regionName,
          this.protocol, method, args));
    }

    return null;
  }

  public BatchExecResult realInvoke() throws Exception {
    if (!this.execList.isEmpty()) {
      String id = UUID.randomUUID().toString();
      return this.connection.getHRegionConnection(
        this.serverName.getHostname(), this.serverName.getPort())
          .execBatchCoprocessor(id, this.execList,
            this.serverCallback);
    }

    return null;
  }
}