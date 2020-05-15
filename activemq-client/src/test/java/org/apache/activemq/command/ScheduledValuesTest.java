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
package org.apache.activemq.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import javax.jms.MessageFormatException;

import org.apache.activemq.ScheduledMessage;
import org.junit.Test;

public class ScheduledValuesTest {

    @Test
    public void testNegativeDelay() throws Exception {
        ActiveMQMessage message = new ActiveMQMessage();
        
        try {
            message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, -1L);
            fail("Failure expected on a negative value");
        } catch (MessageFormatException ex) {
            assertEquals("AMQ_SCHEDULED_DELAY must not be a negative value", ex.getMessage());
        }
    }
    
    @Test
    public void testNegativeRepeat() throws Exception {
        ActiveMQMessage message = new ActiveMQMessage();
        
        try {
            message.setIntProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT, -1);
            fail("Failure expected on a negative value");
        } catch (MessageFormatException ex) {
            assertEquals("AMQ_SCHEDULED_REPEAT must not be a negative value", ex.getMessage());
        }
    }
    
    @Test
    public void testNegativePeriod() throws Exception {
        ActiveMQMessage message = new ActiveMQMessage();
        
        try {
            message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD, -1L);
            fail("Failure expected on a negative value");
        } catch (MessageFormatException ex) {
            assertEquals("AMQ_SCHEDULED_PERIOD must not be a negative value", ex.getMessage());
        }
    }
    
    @Test
    public void testScheduledDelayViaCron() throws Exception {
        ActiveMQMessage message = new ActiveMQMessage();
        
        try {
            message.setStringProperty(ScheduledMessage.AMQ_SCHEDULED_CRON, "-1 * * * *");
            fail("Failure expected on a negative value");
        } catch (NumberFormatException ex) {
            // expected
        }
    }
}
