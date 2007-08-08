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
package org.apache.activemq.broker.util;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.MulticastSocket;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * A Broker interceptor which allows you to trace all operations to a Multicast
 * socket.
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision: 427613 $
 */
public class MulticastTraceBrokerPlugin extends UDPTraceBrokerPlugin {

    private int timeToLive = 1;

    public MulticastTraceBrokerPlugin() {
        try {
            destination = new URI("multicast://224.1.2.3:61616");
        } catch (URISyntaxException wontHappen) {
        }
    }

    protected DatagramSocket createSocket() throws IOException {
        MulticastSocket s = new MulticastSocket();
        s.setSendBufferSize(maxTraceDatagramSize);
        s.setBroadcast(broadcast);
        s.setLoopbackMode(true);
        s.setTimeToLive(timeToLive);
        return s;
    }

    public int getTimeToLive() {
        return timeToLive;
    }

    public void setTimeToLive(int timeToLive) {
        this.timeToLive = timeToLive;
    }

}
