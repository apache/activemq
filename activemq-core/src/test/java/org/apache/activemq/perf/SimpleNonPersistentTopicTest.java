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
package org.apache.activemq.perf;

import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;

/**
 * @version $Revision: 1.3 $
 */
public class SimpleNonPersistentTopicTest extends SimpleTopicTest {

    protected PerfProducer createProducer(ConnectionFactory fac, Destination dest, int number, byte[] payload) throws JMSException {
        PerfProducer pp = new PerfProducer(fac, dest, payload);
        pp.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        return pp;
    }
}
