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
package my.test.client;

import my.test.TestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class PutTest extends TestBase implements Callable<Void> {
    public static void main(String[] args) throws Exception {
        //new PutTest().call();
        List<PutTest> t = new ArrayList<PutTest>();
        t.add(new PutTest());
        t.add(new PutTest());
        t.add(new PutTest());
        t.add(new PutTest());
        t.add(new PutTest());
        t.add(new PutTest());
        t.add(new PutTest());
        t.add(new PutTest());
        t.add(new PutTest());
        t.add(new PutTest());
        executeUpdate(t);
    }

    private final static ThreadPoolExecutor pool = getPool();

    private static ThreadPoolExecutor getPool() {
        //TODO 可配置的线程池 参数
        ThreadPoolExecutor pool = new ThreadPoolExecutor(1, 20, 5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
                Threads.newDaemonThreadFactory(PutTest.class.getSimpleName()));
        pool.allowCoreThreadTimeOut(true);

        return pool;
    }

    public static void executeUpdate(List<PutTest> commands) {
        if (commands.size() == 1) {
            PutTest c = commands.get(0);
            try {
                c.call();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        int size = commands.size();
        List<Future<Void>> futures = new ArrayList<Future<Void>>(size);
        for (int i = 0; i < size; i++) {
            final PutTest c = commands.get(i);
            futures.add(pool.submit(c));
        }
        try {
            for (int i = 0; i < size; i++) {
                futures.get(i).get();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    byte[] cf = toB("CF");
    byte[] id = toB("ID");
    byte[] name = toB("NAME");
    byte[] age = toB("AGE");
    byte[] salary = toB("SALARY");

    Configuration conf = HBaseConfiguration.create();
    HTableInterface t;
    int startKey = 1000;
    int endKey = 1500;
    int loop = 10;

    @Override
    public Void call() throws Exception {
        tableName = "PutTest";
        try {
            //createTable("CF");

            t = getHTable();
            for (int i = 0; i < loop; i++) {
                total += testHBase();
            }
            avg();
        } finally {
            // deleteTable(tableName);
        }
        return null;
    }

    long testHBase() throws Exception {
        long start = System.nanoTime();
        for (int i = startKey; i < endKey; i++) {
            Put put = new Put(b("RK" + i));
            put.add(cf, id, b(i));
            put.add(cf, name, b("zhh-2009"));
            put.add(cf, age, b(30L));
            put.add(cf, salary, b(3000.50F));
            t.put(put);
        }
        long end = System.nanoTime();
        p("testHBase()", end - start);

        return end - start;
    }

    byte[] b(String v) {
        return Bytes.toBytes(v);
    }

    byte[] b(long v) {
        return Bytes.toBytes(v);
    }

    byte[] b(int v) {
        return Bytes.toBytes(v);
    }

    byte[] b(float v) {
        return Bytes.toBytes(v);
    }

    long total = 0;

    void avg() {
        p("----------------------------");
        p("rows: " + (endKey - startKey) + ", loop: " + loop + ", avg", total / loop);
        p();
        total = 0;
    }

    void p(String m, long v) {
        System.out.println(m + ": " + v / 1000000 + " ms");
    }

    void p(String str) {
        System.out.println(str);
    }

}