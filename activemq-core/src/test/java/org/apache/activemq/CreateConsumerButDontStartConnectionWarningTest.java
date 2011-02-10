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
package org.apache.activemq;

import javax.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version $Revision: 1.1 $
 */
public class CreateConsumerButDontStartConnectionWarningTest extends JmsQueueSendReceiveTest {
    private static final transient Logger LOG = LoggerFactory.getLogger(CreateConsumerButDontStartConnectionWarningTest.class);

    @Override
    protected void startConnection() throws JMSException {
        // don't start the connection
    }

    @Override
    protected void assertMessagesAreReceived() throws JMSException {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            LOG.warn("Caught: " + e, e);
        }
    }
}
