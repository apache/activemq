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
package org.apache.activemq.transport.amqp.message;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

public interface JMSVendor {

    public abstract BytesMessage createBytesMessage();

    public abstract StreamMessage createStreamMessage();

    public abstract Message createMessage();

    public abstract TextMessage createTextMessage();

    public abstract ObjectMessage createObjectMessage();

    public abstract MapMessage createMapMessage();

    public abstract void setJMSXUserID(Message message, String value);

    public Destination createDestination(String name);

    public abstract void setJMSXGroupID(Message message, String groupId);

    public abstract void setJMSXGroupSequence(Message message, int value);

    public abstract void setJMSXDeliveryCount(Message message, long value);

    public abstract String toAddress(Destination destination);

}
