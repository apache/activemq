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
package org.apache.activemq.console.filter;

import java.lang.reflect.Method;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQStreamMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.console.formatter.GlobalWriter;
import org.apache.activemq.console.util.AmqMessagesUtil;

public class MapTransformFilter extends ResultTransformFilter {
    /**
     * Creates a Map transform filter that is able to transform a variety of
     * objects to a properties map object
     * 
     * @param next - the next query filter
     */
    public MapTransformFilter(QueryFilter next) {
        super(next);
    }

    /**
     * Transform the given object to a Map object
     * 
     * @param object - object to transform
     * @return map object
     */
    protected Object transformElement(Object object) throws Exception {
        // Use reflection to determine how the object should be transformed
        try {
            Method method = this.getClass().getDeclaredMethod("transformToMap", new Class[] {
                object.getClass()
            });
            return (Map)method.invoke(this, new Object[] {
                object
            });
        } catch (NoSuchMethodException e) {
            GlobalWriter.print("Unable to transform mbean of type: " + object.getClass().getName() + ". No corresponding transformToMap method found.");
            return null;
        }
    }

    /**
     * Transform an ObjectInstance mbean to a Map
     * 
     * @param obj - ObjectInstance format of an mbean
     * @return map object
     */
    protected Map transformToMap(ObjectInstance obj) {
        return transformToMap(obj.getObjectName());
    }

    /**
     * Transform an ObjectName mbean to a Map
     * 
     * @param objname - ObjectName format of an mbean
     * @return map object
     */
    protected Map transformToMap(ObjectName objname) {
        Properties props = new Properties();

        // Parse object properties
        Map objProps = objname.getKeyPropertyList();
        for (Iterator i = objProps.keySet().iterator(); i.hasNext();) {
            Object key = i.next();
            Object val = objProps.get(key);
            if (val != null) {
                props.setProperty(key.toString(), val.toString());
            }
        }

        return props;
    }

    /**
     * Transform an Attribute List format of an mbean to a Map
     * 
     * @param list - AttributeList format of an mbean
     * @return map object
     */
    protected Map transformToMap(AttributeList list) {
        Properties props = new Properties();
        for (Iterator i = list.iterator(); i.hasNext();) {
            Attribute attrib = (Attribute)i.next();

            // If attribute is an ObjectName
            if (attrib.getName().equals(MBeansAttributeQueryFilter.KEY_OBJECT_NAME_ATTRIBUTE)) {
                props.putAll(transformToMap((ObjectName)attrib.getValue()));
            } else {
                if (attrib.getValue() != null) {
                    props.setProperty(attrib.getName(), attrib.getValue().toString());
                }
            }
        }

        return props;
    }

    /**
     * Transform an ActiveMQTextMessage to a Map
     * 
     * @param msg - text message to trasnform
     * @return map object
     * @throws JMSException
     */
    protected Map transformToMap(ActiveMQTextMessage msg) throws JMSException {
        Properties props = new Properties();

        props.putAll(transformToMap((ActiveMQMessage)msg));
        if (msg.getText() != null) {
            props.setProperty(AmqMessagesUtil.JMS_MESSAGE_BODY_PREFIX + "JMSText", msg.getText());
        }

        return props;
    }

    /**
     * Transform an ActiveMQBytesMessage to a Map
     * 
     * @param msg - bytes message to transform
     * @return map object
     * @throws JMSException
     */
    protected Map transformToMap(ActiveMQBytesMessage msg) throws JMSException {
        Properties props = new Properties();

        props.putAll(transformToMap((ActiveMQMessage)msg));

        long bodyLength = msg.getBodyLength();
        byte[] msgBody;
        int i = 0;
        // Create separate bytes messages
        for (i = 0; i < (bodyLength / Integer.MAX_VALUE); i++) {
            msgBody = new byte[Integer.MAX_VALUE];
            props.setProperty(AmqMessagesUtil.JMS_MESSAGE_BODY_PREFIX + "JMSBytes:" + (i + 1), new String(msgBody));
        }
        msgBody = new byte[(int)(bodyLength % Integer.MAX_VALUE)];
        props.setProperty(AmqMessagesUtil.JMS_MESSAGE_BODY_PREFIX + "JMSBytes:" + (i + 1), new String(msgBody));

        return props;
    }

    /**
     * Transform an ActiveMQMessage to a Map
     * 
     * @param msg - object message to transform
     * @return map object
     * @throws JMSException
     */
    protected Map transformToMap(ActiveMQObjectMessage msg) throws JMSException {
        Properties props = new Properties();

        props.putAll(transformToMap((ActiveMQMessage)msg));
        if (msg.getObject() != null) {
            // Just add the class name and toString value of the object
            props.setProperty(AmqMessagesUtil.JMS_MESSAGE_BODY_PREFIX + "JMSObjectClass", msg.getObject().getClass().getName());
            props.setProperty(AmqMessagesUtil.JMS_MESSAGE_BODY_PREFIX + "JMSObjectString", msg.getObject().toString());
        }
        return props;
    }

    /**
     * Transform an ActiveMQMapMessage to a Map
     * 
     * @param msg - map message to transform
     * @return map object
     * @throws JMSException
     */
    protected Map transformToMap(ActiveMQMapMessage msg) throws JMSException {
        Properties props = new Properties();

        props.putAll(transformToMap((ActiveMQMessage)msg));

        // Get map properties
        Enumeration e = msg.getMapNames();
        while (e.hasMoreElements()) {
            String key = (String)e.nextElement();
            Object val = msg.getObject(key);
            if (val != null) {
                props.setProperty(AmqMessagesUtil.JMS_MESSAGE_BODY_PREFIX + key, val.toString());
            }
        }

        return props;
    }

    /**
     * Transform an ActiveMQStreamMessage to a Map
     * 
     * @param msg - stream message to transform
     * @return map object
     * @throws JMSException
     */
    protected Map transformToMap(ActiveMQStreamMessage msg) throws JMSException {
        Properties props = new Properties();

        props.putAll(transformToMap((ActiveMQMessage)msg));
        // Just set the toString of the message as the body of the stream
        // message
        props.setProperty(AmqMessagesUtil.JMS_MESSAGE_BODY_PREFIX + "JMSStreamMessage", msg.toString());

        return props;
    }

    /**
     * Transform an ActiveMQMessage to a Map
     * 
     * @param msg - message to transform
     * @return map object
     * @throws JMSException
     */
    protected Map<String, String> transformToMap(ActiveMQMessage msg) throws JMSException {
        Map<String, String> props = new HashMap<String, String>();

        // Get JMS properties
        if (msg.getJMSCorrelationID() != null) {
            props.put(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + "JMSCorrelationID", msg.getJMSCorrelationID());
        }
        props.put(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + "JMSDeliveryMode", (msg.getJMSDeliveryMode() == DeliveryMode.PERSISTENT) ? "persistent" : "non-persistent");
        if (msg.getJMSDestination() != null) {
            props.put(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + "JMSDestination", ((ActiveMQDestination)msg.getJMSDestination()).getPhysicalName());
        }
        props.put(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + "JMSExpiration", Long.toString(msg.getJMSExpiration()));
        props.put(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + "JMSMessageID", msg.getJMSMessageID());
        props.put(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + "JMSPriority", Integer.toString(msg.getJMSPriority()));
        props.put(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + "JMSRedelivered", Boolean.toString(msg.getJMSRedelivered()));
        if (msg.getJMSReplyTo() != null) {
            props.put(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + "JMSReplyTo", ((ActiveMQDestination)msg.getJMSReplyTo()).getPhysicalName());
        }
        props.put(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + "JMSTimestamp", Long.toString(msg.getJMSTimestamp()));
        if (msg.getJMSType() != null) {
            props.put(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + "JMSType", msg.getJMSType());
        }

        // Get custom properties
        Enumeration e = msg.getPropertyNames();
        while (e.hasMoreElements()) {
            String name = (String)e.nextElement();
            if (msg.getObjectProperty(name) != null) {
                props.put(AmqMessagesUtil.JMS_MESSAGE_CUSTOM_PREFIX + name, msg.getObjectProperty(name).toString());
            }
        }

        return props;
    }

    /**
     * Transform an openMBean composite data to a Map
     * 
     * @param data - composite data to transform
     * @return map object
     */
    protected Map transformToMap(CompositeDataSupport data) {
        Properties props = new Properties();

        String typeName = data.getCompositeType().getTypeName();

        // Retrieve text message
        if (typeName.equals(ActiveMQTextMessage.class.getName())) {
            props.setProperty(AmqMessagesUtil.JMS_MESSAGE_BODY_PREFIX + "Text", data.get("Text").toString());

            // Retrieve byte preview
        } else if (typeName.equals(ActiveMQBytesMessage.class.getName())) {
            props.setProperty(AmqMessagesUtil.JMS_MESSAGE_BODY_PREFIX + "BodyLength", data.get("BodyLength").toString());
            props.setProperty(AmqMessagesUtil.JMS_MESSAGE_BODY_PREFIX + "BodyPreview", new String((byte[])data.get("BodyPreview")));

            // Expand content map
        } else if (typeName.equals(ActiveMQMapMessage.class.getName())) {
            Map contentMap = (Map)data.get("ContentMap");
            for (Iterator i = contentMap.keySet().iterator(); i.hasNext();) {
                String key = (String)i.next();
                props.setProperty(AmqMessagesUtil.JMS_MESSAGE_BODY_PREFIX + key, contentMap.get(key).toString());
            }

            // Do nothing
        } else if (typeName.equals(ActiveMQObjectMessage.class.getName()) || typeName.equals(ActiveMQStreamMessage.class.getName()) || typeName.equals(ActiveMQMessage.class.getName())) {

            // Unrecognized composite data. Throw exception.
        } else {
            throw new IllegalArgumentException("Unrecognized composite data to transform. composite type: " + typeName);
        }

        // Process the JMS message header values
        props.setProperty(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + "JMSCorrelationID", "" + data.get("JMSCorrelationID"));
        props.setProperty(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + "JMSDestination", "" + data.get("JMSDestination"));
        props.setProperty(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + "JMSMessageID", "" + data.get("JMSMessageID"));
        props.setProperty(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + "JMSReplyTo", "" + data.get("JMSReplyTo"));
        props.setProperty(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + "JMSType", "" + data.get("JMSType"));
        props.setProperty(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + "JMSDeliveryMode", "" + data.get("JMSDeliveryMode"));
        props.setProperty(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + "JMSExpiration", "" + data.get("JMSExpiration"));
        props.setProperty(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + "JMSPriority", "" + data.get("JMSPriority"));
        props.setProperty(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + "JMSRedelivered", "" + data.get("JMSRedelivered"));
        props.setProperty(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + "JMSTimestamp", "" + data.get("JMSTimestamp"));

        // Process the JMS custom message properties
        props.setProperty(AmqMessagesUtil.JMS_MESSAGE_CUSTOM_PREFIX + "Properties", "" + data.get("Properties"));

        return props;
    }
}
