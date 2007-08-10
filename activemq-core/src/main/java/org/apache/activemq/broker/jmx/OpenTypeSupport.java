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
package org.apache.activemq.broker.jmx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQStreamMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.Message;

public final class OpenTypeSupport {

    interface OpenTypeFactory {
        CompositeType getCompositeType() throws OpenDataException;

        Map<String, Object> getFields(Object o) throws OpenDataException;
    }

    private static final Map<Class, MessageOpenTypeFactory> OPEN_TYPE_FACTORIES = new HashMap<Class, MessageOpenTypeFactory>();

    abstract static class AbstractOpenTypeFactory implements OpenTypeFactory {

        private CompositeType compositeType;
        private List<String> itemNamesList = new ArrayList<String>();
        private List<String> itemDescriptionsList = new ArrayList<String>();
        private List<OpenType> itemTypesList = new ArrayList<OpenType>();

        public CompositeType getCompositeType() throws OpenDataException {
            if (compositeType == null) {
                init();
                compositeType = createCompositeType();
            }
            return compositeType;
        }

        protected void init() throws OpenDataException {
        }

        protected CompositeType createCompositeType() throws OpenDataException {
            String[] itemNames = itemNamesList.toArray(new String[itemNamesList.size()]);
            String[] itemDescriptions = itemDescriptionsList.toArray(new String[itemDescriptionsList.size()]);
            OpenType[] itemTypes = itemTypesList.toArray(new OpenType[itemTypesList.size()]);
            return new CompositeType(getTypeName(), getDescription(), itemNames, itemDescriptions, itemTypes);
        }

        protected abstract String getTypeName();

        protected void addItem(String name, String description, OpenType type) {
            itemNamesList.add(name);
            itemDescriptionsList.add(description);
            itemTypesList.add(type);
        }

        protected String getDescription() {
            return getTypeName();
        }

        public Map<String, Object> getFields(Object o) throws OpenDataException {
            Map<String, Object> rc = new HashMap<String, Object>();
            return rc;
        }
    }

    static class MessageOpenTypeFactory extends AbstractOpenTypeFactory {

        protected String getTypeName() {
            return ActiveMQMessage.class.getName();
        }

        protected void init() throws OpenDataException {
            super.init();
            addItem("JMSCorrelationID", "JMSCorrelationID", SimpleType.STRING);
            addItem("JMSDestination", "JMSDestination", SimpleType.STRING);
            addItem("JMSMessageID", "JMSMessageID", SimpleType.STRING);
            addItem("JMSReplyTo", "JMSReplyTo", SimpleType.STRING);
            addItem("JMSType", "JMSType", SimpleType.STRING);
            addItem("JMSDeliveryMode", "JMSDeliveryMode", SimpleType.STRING);
            addItem("JMSExpiration", "JMSExpiration", SimpleType.LONG);
            addItem("JMSPriority", "JMSPriority", SimpleType.INTEGER);
            addItem("JMSRedelivered", "JMSRedelivered", SimpleType.BOOLEAN);
            addItem("JMSTimestamp", "JMSTimestamp", SimpleType.DATE);
            addItem("Properties", "Properties", SimpleType.STRING);
        }

        public Map<String, Object> getFields(Object o) throws OpenDataException {
            ActiveMQMessage m = (ActiveMQMessage)o;
            Map<String, Object> rc = super.getFields(o);
            rc.put("JMSCorrelationID", m.getJMSCorrelationID());
            rc.put("JMSDestination", "" + m.getJMSDestination());
            rc.put("JMSMessageID", m.getJMSMessageID());
            rc.put("JMSReplyTo", "" + m.getJMSReplyTo());
            rc.put("JMSType", m.getJMSType());
            rc.put("JMSDeliveryMode", m.getJMSDeliveryMode() == DeliveryMode.PERSISTENT ? "PERSISTENT" : "NON-PERSISTENT");
            rc.put("JMSExpiration", Long.valueOf(m.getJMSExpiration()));
            rc.put("JMSPriority", Integer.valueOf(m.getJMSPriority()));
            rc.put("JMSRedelivered", Boolean.valueOf(m.getJMSRedelivered()));
            rc.put("JMSTimestamp", new Date(m.getJMSTimestamp()));
            try {
                rc.put("Properties", "" + m.getProperties());
            } catch (IOException e) {
                rc.put("Properties", "");
            }
            return rc;
        }
    }

    static class ByteMessageOpenTypeFactory extends MessageOpenTypeFactory {

        protected String getTypeName() {
            return ActiveMQBytesMessage.class.getName();
        }

        protected void init() throws OpenDataException {
            super.init();
            addItem("BodyLength", "Body length", SimpleType.LONG);
            addItem("BodyPreview", "Body preview", new ArrayType(1, SimpleType.BYTE));
        }

        public Map<String, Object> getFields(Object o) throws OpenDataException {
            ActiveMQBytesMessage m = (ActiveMQBytesMessage)o;
            Map<String, Object> rc = super.getFields(o);
            long length = 0;
            try {
                length = m.getBodyLength();
                rc.put("BodyLength", Long.valueOf(length));
            } catch (JMSException e) {
                rc.put("BodyLength", Long.valueOf(0));
            }
            try {
                byte preview[] = new byte[(int)Math.min(length, 255)];
                m.readBytes(preview);

                // This is whack! Java 1.5 JMX spec does not support primitive
                // arrays!
                // In 1.6 it seems it is supported.. but until then...
                Byte data[] = new Byte[preview.length];
                for (int i = 0; i < data.length; i++) {
                    data[i] = new Byte(preview[i]);
                }

                rc.put("BodyPreview", data);
            } catch (JMSException e) {
                rc.put("BodyPreview", new byte[] {});
            }
            return rc;
        }

    }

    static class MapMessageOpenTypeFactory extends MessageOpenTypeFactory {
        protected String getTypeName() {
            return ActiveMQMapMessage.class.getName();
        }

        protected void init() throws OpenDataException {
            super.init();
            addItem("ContentMap", "Content map", SimpleType.STRING);
        }

        public Map<String, Object> getFields(Object o) throws OpenDataException {
            ActiveMQMapMessage m = (ActiveMQMapMessage)o;
            Map<String, Object> rc = super.getFields(o);
            try {
                rc.put("ContentMap", "" + m.getContentMap());
            } catch (JMSException e) {
                rc.put("ContentMap", "");
            }
            return rc;
        }
    }

    static class ObjectMessageOpenTypeFactory extends MessageOpenTypeFactory {
        protected String getTypeName() {
            return ActiveMQObjectMessage.class.getName();
        }

        protected void init() throws OpenDataException {
            super.init();
        }

        public Map<String, Object> getFields(Object o) throws OpenDataException {
            Map<String, Object> rc = super.getFields(o);
            return rc;
        }
    }

    static class StreamMessageOpenTypeFactory extends MessageOpenTypeFactory {
        protected String getTypeName() {
            return ActiveMQStreamMessage.class.getName();
        }

        protected void init() throws OpenDataException {
            super.init();
        }

        public Map<String, Object> getFields(Object o) throws OpenDataException {
            Map<String, Object> rc = super.getFields(o);
            return rc;
        }
    }

    static class TextMessageOpenTypeFactory extends MessageOpenTypeFactory {
        protected String getTypeName() {
            return ActiveMQTextMessage.class.getName();
        }

        protected void init() throws OpenDataException {
            super.init();
            addItem("Text", "Text", SimpleType.STRING);
        }

        public Map<String, Object> getFields(Object o) throws OpenDataException {
            ActiveMQTextMessage m = (ActiveMQTextMessage)o;
            Map<String, Object> rc = super.getFields(o);
            try {
                rc.put("Text", "" + m.getText());
            } catch (JMSException e) {
                rc.put("Text", "");
            }
            return rc;
        }
    }

    static {
        OPEN_TYPE_FACTORIES.put(ActiveMQMessage.class, new MessageOpenTypeFactory());
        OPEN_TYPE_FACTORIES.put(ActiveMQBytesMessage.class, new ByteMessageOpenTypeFactory());
        OPEN_TYPE_FACTORIES.put(ActiveMQMapMessage.class, new MapMessageOpenTypeFactory());
        OPEN_TYPE_FACTORIES.put(ActiveMQObjectMessage.class, new ObjectMessageOpenTypeFactory());
        OPEN_TYPE_FACTORIES.put(ActiveMQStreamMessage.class, new StreamMessageOpenTypeFactory());
        OPEN_TYPE_FACTORIES.put(ActiveMQTextMessage.class, new TextMessageOpenTypeFactory());
    }

    private OpenTypeSupport() {
    }
    
    public static OpenTypeFactory getFactory(Class<? extends Message> clazz) throws OpenDataException {
        return OPEN_TYPE_FACTORIES.get(clazz);
    }

    public static CompositeData convert(Message message) throws OpenDataException {
        OpenTypeFactory f = getFactory(message.getClass());
        if (f == null) {
            throw new OpenDataException("Cannot create a CompositeData for type: " + message.getClass().getName());
        }
        CompositeType ct = f.getCompositeType();
        Map<String, Object> fields = f.getFields(message);
        return new CompositeDataSupport(ct, fields);
    }

}
