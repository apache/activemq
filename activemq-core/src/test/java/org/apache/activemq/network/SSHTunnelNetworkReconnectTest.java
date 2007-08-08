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
package org.apache.activemq.network;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Test network reconnects over SSH tunnels. This case can be especially tricky
 * since the SSH tunnels fool the TCP transport into thinking that they are
 * initially connected.
 * 
 * @author chirino
 */
public class SSHTunnelNetworkReconnectTest extends NetworkReconnectTest {
    private static final transient Log log = LogFactory.getLog(SSHTunnelNetworkReconnectTest.class);

    ArrayList processes = new ArrayList();

    protected BrokerService createFirstBroker() throws Exception {
        return BrokerFactory
            .createBroker(new URI("xbean:org/apache/activemq/network/ssh-reconnect-broker1.xml"));
    }

    protected BrokerService createSecondBroker() throws Exception {
        return BrokerFactory
            .createBroker(new URI("xbean:org/apache/activemq/network/ssh-reconnect-broker2.xml"));
    }

    protected void setUp() throws Exception {
        startProcess("ssh -Nn -L60006:localhost:61616 localhost");
        startProcess("ssh -Nn -L60007:localhost:61617 localhost");
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        for (Iterator iter = processes.iterator(); iter.hasNext();) {
            Process p = (Process)iter.next();
            p.destroy();
        }
    }

    private void startProcess(String command) throws IOException {
        final Process process = Runtime.getRuntime().exec(command);
        processes.add(process);
        new Thread("stdout: " + command) {
            public void run() {
                try {
                    InputStream is = process.getInputStream();
                    int c;
                    while ((c = is.read()) >= 0) {
                        System.out.write(c);
                    }
                } catch (IOException e) {
                }
            }
        }.start();
        new Thread("stderr: " + command) {
            public void run() {
                try {
                    InputStream is = process.getErrorStream();
                    int c;
                    while ((c = is.read()) >= 0) {
                        System.err.write(c);
                    }
                } catch (IOException e) {
                }
            }
        }.start();
    }
}
