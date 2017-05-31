/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.leveldb.test;

import org.apache.activemq.leveldb.replicated.MasterLevelDBStore;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;


/**
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
public class MasterLevelDBStoreTest {

    MasterLevelDBStore store;

    @Test(timeout = 1000*60*10)
    public void testStoppingStoreStopsTransport() throws Exception {
        store = new MasterLevelDBStore();
        store.setDirectory(new File("target/activemq-data/master-leveldb-store-test"));
        store.setReplicas(0);

        ExecutorService threads = Executors.newFixedThreadPool(1);
        threads.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    store.start();
                } catch (Exception e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        });

        // give some time to come up..
        Thread.sleep(2000);
        String address = store.transport_server().getBoundAddress();
        URI bindAddress = new URI(address);
        System.out.println(address);
        Socket socket = new Socket();
        try {
            socket.bind(new InetSocketAddress(bindAddress.getHost(), bindAddress.getPort()));
            fail("We should not have been able to connect...");
        } catch (BindException e) {
            System.out.println("Good. We cannot bind.");
        }


        threads.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    store.stop();
                } catch (Exception e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        });

        Thread.sleep(2000);
        try {
            socket.bind(new InetSocketAddress(bindAddress.getHost(), bindAddress.getPort()));
            System.out.println("Can bind, so protocol server must have been shut down.");

        } catch (IllegalStateException e) {
            fail("Server protocol port is still opened..");
        }

    }

    @After
    public void stop() throws Exception {
        if (store.isStarted()) {
            store.stop();
            FileUtils.deleteQuietly(store.directory());
        }
    }
}
