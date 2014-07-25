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
package org.apache.activemq.transport.failover;

import junit.framework.Test;

import org.apache.activemq.transport.tcp.TransportUriTest;

public class FailoverUriTest extends TransportUriTest {

    @Override
    public void initCombosForTestUriOptionsWork() {
        addCombinationValues("prefix", new Object[]{"failover:(", "failover://("});
        addCombinationValues("postfix", new Object[] {")?initialReconnectDelay=1000&maxReconnectDelay=1000"
                , "?wireFormat.tightEncodingEnabled=false)?jms.useAsyncSend=true&jms.copyMessageOnSend=false"
                , "?wireFormat.maxInactivityDuration=0&keepAlive=true)?jms.prefetchPolicy.all=500&initialReconnectDelay=10000&useExponentialBackOff=false&maxReconnectAttempts=0&randomize=false"});
    }

    @Override
    public void initCombosForTestBadVersionNumberDoesNotWork() {
        addCombinationValues("prefix", new Object[]{"failover:("});
        addCombinationValues("postfix", new Object[] {")?initialReconnectDelay=1000&maxReconnectDelay=1000"});
    }

    @Override
    public void initCombosForTestBadPropertyNameFails() {
        addCombinationValues("prefix", new Object[]{"failover:("});
        addCombinationValues("postfix", new Object[] {")?initialReconnectDelay=1000&maxReconnectDelay=1000"});
    }

    public static Test suite() {
        return suite(FailoverUriTest.class);
    }
}
