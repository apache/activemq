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
package org.apache.activemq.shiro.authz;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @since 5.10.0
 */
public class DestinationActionTest {

    @Test(expected = IllegalArgumentException.class)
    public void testNullConnectionContext() {
        new DestinationAction(null, new ActiveMQQueue("foo"), "create");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullDestination() {
        new DestinationAction(new ConnectionContext(), null, "create");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullVerb() {
        new DestinationAction(new ConnectionContext(), new ActiveMQQueue("foo"), null);
    }

    @Test
    public void testDefault() {
        ConnectionContext ctx = new ConnectionContext();
        ActiveMQQueue queue = new ActiveMQQueue("foo");
        String verb = "create";

        DestinationAction action = new DestinationAction(ctx, queue, verb);
        assertSame(ctx, action.getConnectionContext());
        assertSame(queue, action.getDestination());
        assertEquals(verb, action.getVerb());
        assertEquals("create destination: queue://foo", action.toString());
    }

}
