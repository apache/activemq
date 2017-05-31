/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.leveldb.test;

import org.apache.activemq.leveldb.CountDownFuture;
import org.apache.activemq.leveldb.util.FileSupport;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.TestServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Created by chirino on 10/30/13.
 */
public class ZooKeeperTestSupport {

    protected TestServerCnxnFactory connector;

    static File data_dir() {
        return new File("target/activemq-data/leveldb-elections");
    }


    @Before
    public void startZooKeeper() throws Exception {
        FileSupport.toRichFile(data_dir()).recursiveDelete();

        System.out.println("Starting ZooKeeper");
        ZooKeeperServer zk_server = new ZooKeeperServer();
        zk_server.setTickTime(500);
        zk_server.setTxnLogFactory(new FileTxnSnapLog(new File(data_dir(), "zk-log"), new File(data_dir(), "zk-data")));
        connector = new TestServerCnxnFactory();
        connector.configure(new InetSocketAddress(0), 100);
        connector.startup(zk_server);
        System.out.println("ZooKeeper started");
    }

    @After
    public void stopZooKeeper() throws Exception {
        if( connector!=null ) {
          connector.shutdown();
          connector = null;
        }
        deleteDirectory("zk-log");
        deleteDirectory("zk-data");
    }


    protected static interface Task {
        public void run() throws Exception;
    }

    protected  void within(int time, TimeUnit unit, Task task) throws InterruptedException {
        long timeMS = unit.toMillis(time);
        long deadline = System.currentTimeMillis() + timeMS;
        while (true) {
            try {
                task.run();
                return;
            } catch (Throwable e) {
                long remaining = deadline - System.currentTimeMillis();
                if( remaining <=0 ) {
                    if( e instanceof RuntimeException ) {
                        throw (RuntimeException)e;
                    }
                    if( e instanceof Error ) {
                        throw (Error)e;
                    }
                    throw new RuntimeException(e);
                }
                Thread.sleep(Math.min(timeMS/10, remaining));
            }
        }
    }

    protected CountDownFuture waitFor(int timeout, CountDownFuture... futures) throws InterruptedException {
        long deadline =  System.currentTimeMillis()+timeout;
        while( true ) {
            for (CountDownFuture f:futures) {
                if( f.await(1, TimeUnit.MILLISECONDS) ) {
                    return f;
                }
            }
            long remaining = deadline - System.currentTimeMillis();
            if( remaining < 0 ) {
                return null;
            } else {
                Thread.sleep(Math.min(remaining / 10, 100L));
            }
        }
    }

    protected void deleteDirectory(String s) throws java.io.IOException {
        try {
            FileUtils.deleteDirectory(new File(data_dir(), s));
        } catch (java.io.IOException e) {
        }
    }
}
