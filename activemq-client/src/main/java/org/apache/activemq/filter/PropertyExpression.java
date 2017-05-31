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

package org.apache.activemq.filter;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.util.JMSExceptionSupport;

/**
 * Represents a property expression
 */
public class PropertyExpression implements Expression {

    private static final Map<String, SubExpression> JMS_PROPERTY_EXPRESSIONS = new HashMap<String, SubExpression>();

    interface SubExpression {
        Object evaluate(Message message);
    }

    static {
        JMS_PROPERTY_EXPRESSIONS.put("JMSDestination", new SubExpression() {

            @Override
            public Object evaluate(Message message) {
                ActiveMQDestination dest = message.getOriginalDestination();
                if (dest == null) {
                    dest = message.getDestination();
                }
                if (dest == null) {
                    return null;
                }
                return dest.toString();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSReplyTo", new SubExpression() {

            @Override
            public Object evaluate(Message message) {
                if (message.getReplyTo() == null) {
                    return null;
                }
                return message.getReplyTo().toString();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSType", new SubExpression() {

            @Override
            public Object evaluate(Message message) {
                return message.getType();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSDeliveryMode", new SubExpression() {

            @Override
            public Object evaluate(Message message) {
                return message.isPersistent() ? "PERSISTENT" : "NON_PERSISTENT";
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSPriority", new SubExpression() {

            @Override
            public Object evaluate(Message message) {
                return Integer.valueOf(message.getPriority());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSMessageID", new SubExpression() {

            @Override
            public Object evaluate(Message message) {
                if (message.getMessageId() == null) {
                    return null;
                }
                return message.getMessageId().toString();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSTimestamp", new SubExpression() {

            @Override
            public Object evaluate(Message message) {
                return Long.valueOf(message.getTimestamp());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSCorrelationID", new SubExpression() {

            @Override
            public Object evaluate(Message message) {
                return message.getCorrelationId();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSExpiration", new SubExpression() {

            @Override
            public Object evaluate(Message message) {
                return Long.valueOf(message.getExpiration());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSRedelivered", new SubExpression() {

            @Override
            public Object evaluate(Message message) {
                return Boolean.valueOf(message.isRedelivered());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSXDeliveryCount", new SubExpression() {

            @Override
            public Object evaluate(Message message) {
                return Integer.valueOf(message.getRedeliveryCounter() + 1);
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSXGroupID", new SubExpression() {

            @Override
            public Object evaluate(Message message) {
                return message.getGroupID();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSXUserID", new SubExpression() {

            @Override
            public Object evaluate(Message message) {
                Object userId = message.getUserID();
                if (userId == null) {
                    try {
                        userId = message.getProperty("JMSXUserID");
                    } catch (IOException e) {
                    }
                }

                return userId;
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSXGroupSeq", new SubExpression() {

            @Override
            public Object evaluate(Message message) {
                return new Integer(message.getGroupSequence());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSXProducerTXID", new SubExpression() {

            @Override
            public Object evaluate(Message message) {
                TransactionId txId = message.getOriginalTransactionId();
                if (txId == null) {
                    txId = message.getTransactionId();
                }
                if (txId == null) {
                    return null;
                }
                return txId.toString();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSActiveMQBrokerInTime", new SubExpression() {

            @Override
            public Object evaluate(Message message) {
                return Long.valueOf(message.getBrokerInTime());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSActiveMQBrokerOutTime", new SubExpression() {

            @Override
            public Object evaluate(Message message) {
                return Long.valueOf(message.getBrokerOutTime());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSActiveMQBrokerPath", new SubExpression() {

            @Override
            public Object evaluate(Message message) {
                return Arrays.toString(message.getBrokerPath());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSXGroupFirstForConsumer", new SubExpression() {

            @Override
            public Object evaluate(Message message) {
                return Boolean.valueOf(message.isJMSXGroupFirstForConsumer());
            }
        });
    }

    private final String name;
    private final SubExpression jmsPropertyExpression;

    public PropertyExpression(String name) {
        this.name = name;
        jmsPropertyExpression = JMS_PROPERTY_EXPRESSIONS.get(name);
    }

    @Override
    public Object evaluate(MessageEvaluationContext message) throws JMSException {
        try {
            if (message.isDropped()) {
                return null;
            }

            if (jmsPropertyExpression != null) {
                return jmsPropertyExpression.evaluate(message.getMessage());
            }
            try {
                return message.getMessage().getProperty(name);
            } catch (IOException ioe) {
                throw JMSExceptionSupport.create("Could not get property: " + name + " reason: " + ioe.getMessage(), ioe);
            }
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }

    }

    public Object evaluate(Message message) throws JMSException {
        if (jmsPropertyExpression != null) {
            return jmsPropertyExpression.evaluate(message);
        }
        try {
            return message.getProperty(name);
        } catch (IOException ioe) {
            throw JMSExceptionSupport.create(ioe);
        }
    }

    public String getName() {
        return name;
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return name;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return name.hashCode();
    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o) {

        if (o == null || !this.getClass().equals(o.getClass())) {
            return false;
        }
        return name.equals(((PropertyExpression) o).name);
    }
}
