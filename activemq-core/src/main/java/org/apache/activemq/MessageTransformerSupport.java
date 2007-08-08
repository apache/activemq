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
import javax.jms.Message;

/**
 * A useful base class for message transformers.
 *
 * @version $Revision$
 */
public abstract class MessageTransformerSupport implements MessageTransformer {

    /**
     * Copies the standard JMS and user defined properties from the givem message to the specified message
     *
     * @param fromMessage the message to take the properties from
     * @param toMesage the message to add the properties to
     * @throws JMSException
     */
    protected void copyProperties(Message fromMessage, Message toMesage) throws JMSException {
        ActiveMQMessageTransformation.copyProperties(fromMessage, toMesage);
    }
}
