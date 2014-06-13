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

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.List;

public abstract interface BatchExecCall<T, R> {
  public abstract R call(T paramT) throws IOException;

  public static abstract interface ClientCallback<R> {
    public abstract void update(ServerName paramServerName,
                                List<byte[]> paramList, R paramR) throws IOException;
  }

  public static abstract interface ServerCallback<R> extends Writable {
    public abstract void update(byte[] paramArrayOfByte, R paramR)
        throws IOException;

    public abstract void init();

    public abstract R getResult();
  }
}