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
package org.apache.activemq.transport.http;

import java.net.Socket;
import java.net.URL;

import javax.net.SocketFactory;

import org.apache.activemq.util.Wait;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static junit.framework.Assert.assertTrue;

public class WaitForJettyListener {
    private static final Log LOG = LogFactory.getLog(WaitForJettyListener.class);
    
    public static void waitForJettySocketToAccept(String bindLocation) throws Exception {
        final URL url = new URL(bindLocation);
        assertTrue("Jetty endpoint is available", Wait.waitFor(new Wait.Condition() {

            public boolean isSatisified() throws Exception {
                boolean canConnect = false;
                try {
                    Socket socket = SocketFactory.getDefault().createSocket(url.getHost(), url.getPort());
                    socket.close();
                    canConnect = true;
                } catch (Exception e) {
                    LOG.warn("verify jettty available, failed to connect to " + url + e);
                }
                return canConnect;
            }}, 60 * 1000));
    }
}
