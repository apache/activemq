/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.activeio;

import org.apache.activemq.transport.TransportBrokerTestSupport;

import junit.framework.Test;

public class TCPActiveIOTransportBrokerTest extends TransportBrokerTestSupport {

    protected String getBindLocation() {
        return "tcp://localhost:0?wireFormat.tcpNoDelayEnabled=true&wireFormat.cacheEnabled=true";
    }

    protected void setUp() throws Exception {
        MAX_WAIT=2000;
        super.setUp();
    }
    
    public static Test suite() {
        return suite(TCPActiveIOTransportBrokerTest.class);
    }
    
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }


}
