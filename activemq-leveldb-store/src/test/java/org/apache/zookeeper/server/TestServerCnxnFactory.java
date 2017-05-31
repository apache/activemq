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
package org.apache.zookeeper.server;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * TestServerCnxnFactory allows a caller to impose an artifical
 * wait on I/O over the ServerCnxn used to communicate with the
 * ZooKeeper server.
 */
public class TestServerCnxnFactory extends NIOServerCnxnFactory {
    protected static final Logger LOG = LoggerFactory.getLogger(TestServerCnxnFactory.class);

    /* testHandle controls whehter or not an artifical wait
     * is imposed when talking to the ZooKeeper server
    */
    public TestHandle testHandle = new TestHandle();

    public TestServerCnxnFactory() throws IOException {
        super();
    }

    protected NIOServerCnxn createConnection(SocketChannel sock, SelectionKey sk) throws IOException {
        return new TestServerCnxn(this.zkServer, sock, sk, this, testHandle);
    }

    /*
     * TestHandle is handed to TestServerCnxn and is used to
     * control the amount of time the TestServerCnxn waits
     * before allowing an I/O operation.
     */
    public class TestHandle {
        private Object mu = new Object();
        private int ioWaitMillis = 0;

        /*
         * Set an artifical I/O wait (in milliseconds) on ServerCnxn and
         * then sleep for the specified number of milliseconds.
         */
        public void setIOWaitMillis(int ioWaitMillis, int sleepMillis) {
            synchronized(mu) {
                this.ioWaitMillis = ioWaitMillis;
            }
            if (sleepMillis > 0) {
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException e) {}
            }
        }

        /*
         * Get the number of milliseconds to wait before
         * allowing ServerCnxn to perform I/O.
         */
        public int getIOWaitMillis() {
            synchronized(mu) {
                return this.ioWaitMillis;
            }
        }
    }

    public class TestServerCnxn extends NIOServerCnxn {
        public TestHandle testHandle;

        public TestServerCnxn(ZooKeeperServer zk, SocketChannel sock, SelectionKey sk, NIOServerCnxnFactory factory, TestHandle testHandle) throws IOException {
            super(zk, sock, sk, factory);
            this.testHandle = testHandle;
        }

        public void doIO(SelectionKey k) throws InterruptedException {
            final int millis = this.testHandle.getIOWaitMillis();
            if (millis > 0) {
                LOG.info("imposing a "+millis+" millisecond wait on ServerCxn: "+this);
                try {
                    Thread.sleep(millis);
                } catch (InterruptedException e) {}
            }
            super.doIO(k);
        }
    }
}
