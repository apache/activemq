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
import java.util.Set;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Destination;
import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularType;
import javax.management.openmbean.TabularDataSupport;

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQStreamMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.Message;
import static org.apache.activemq.broker.jmx.CompositeDataConstants.*;

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
        protected TabularType stringPropertyTabularType;
        protected TabularType booleanPropertyTabularType;
        protected TabularType bytePropertyTabularType;
        protected TabularType shortPropertyTabularType;
        protected TabularType intPropertyTabularType;
        protected TabularType longPropertyTabularType;
        protected TabularType floatPropertyTabularType;
        protected TabularType doublePropertyTabularType;

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
            addItem(JMSXGROUP_ID, "Message Group ID", SimpleType.STRING);
            addItem(JMSXGROUP_SEQ, "Message Group Sequence Number", SimpleType.INTEGER);
            addItem(CompositeDataConstants.PROPERTIES, "User Properties Text", SimpleType.STRING);

            // now lets expose the type safe properties
            stringPropertyTabularType = createTabularType(String.class, SimpleType.STRING);
            booleanPropertyTabularType = createTabularType(Boolean.class, SimpleType.BOOLEAN);
            bytePropertyTabularType = createTabularType(Byte.class, SimpleType.BYTE);
            shortPropertyTabularType = createTabularType(Short.class, SimpleType.SHORT);
            intPropertyTabularType = createTabularType(Integer.class, SimpleType.INTEGER);
            longPropertyTabularType = createTabularType(Long.class, SimpleType.LONG);
            floatPropertyTabularType = createTabularType(Float.class, SimpleType.FLOAT);
            doublePropertyTabularType = createTabularType(Double.class, SimpleType.DOUBLE);

            addItem(CompositeDataConstants.STRING_PROPERTIES, "User String Properties", stringPropertyTabularType);
            addItem(CompositeDataConstants.BOOLEAN_PROPERTIES, "User Boolean Properties", booleanPropertyTabularType);
            addItem(CompositeDataConstants.BYTE_PROPERTIES, "User Byte Properties", bytePropertyTabularType);
            addItem(CompositeDataConstants.SHORT_PROPERTIES, "User Short Properties", shortPropertyTabularType);
            addItem(CompositeDataConstants.INT_PROPERTIES, "User Integer Properties", intPropertyTabularType);
            addItem(CompositeDataConstants.LONG_PROPERTIES, "User Long Properties", longPropertyTabularType);
            addItem(CompositeDataConstants.FLOAT_PROPERTIES, "User Float Properties", floatPropertyTabularType);
            addItem(CompositeDataConstants.DOUBLE_PROPERTIES, "User Double Properties", doublePropertyTabularType);
        }

        public Map<String, Object> getFields(Object o) throws OpenDataException {
            ActiveMQMessage m = (ActiveMQMessage)o;
            Map<String, Object> rc = super.getFields(o);
            rc.put("JMSCorrelationID", m.getJMSCorrelationID());
            rc.put("JMSDestination", "" + m.getJMSDestination());
            rc.put("JMSMessageID", m.getJMSMessageID());
            rc.put("JMSReplyTo",toString(m.getJMSReplyTo()));
            rc.put("JMSType", m.getJMSType());
            rc.put("JMSDeliveryMode", m.getJMSDeliveryMode() == DeliveryMode.PERSISTENT ? "PERSISTENT" : "NON-PERSISTENT");
            rc.put("JMSExpiration", Long.valueOf(m.getJMSExpiration()));
            rc.put("JMSPriority", Integer.valueOf(m.getJMSPriority()));
            rc.put("JMSRedelivered", Boolean.valueOf(m.getJMSRedelivered()));
            rc.put("JMSTimestamp", new Date(m.getJMSTimestamp()));
            rc.put(JMSXGROUP_ID, m.getGroupID());
            rc.put(JMSXGROUP_SEQ, m.getGroupSequence());
            try {
                rc.put(CompositeDataConstants.PROPERTIES, "" + m.getProperties());
            } catch (IOException e) {
                rc.put(CompositeDataConstants.PROPERTIES, "");
            }

            try {
                rc.put(CompositeDataConstants.STRING_PROPERTIES, createTabularData(m, stringPropertyTabularType, String.class));
            } catch (IOException e) {
                rc.put(CompositeDataConstants.STRING_PROPERTIES, new TabularDataSupport(stringPropertyTabularType));
            }
            try {
                rc.put(CompositeDataConstants.BOOLEAN_PROPERTIES, createTabularData(m, booleanPropertyTabularType, Boolean.class));
            } catch (IOException e) {
                rc.put(CompositeDataConstants.BOOLEAN_PROPERTIES, new TabularDataSupport(booleanPropertyTabularType));
            }
            try {
                rc.put(CompositeDataConstants.BYTE_PROPERTIES, createTabularData(m, bytePropertyTabularType, Byte.class));
            } catch (IOException e) {
                rc.put(CompositeDataConstants.BYTE_PROPERTIES, new TabularDataSupport(bytePropertyTabularType));
            }
            try {
                rc.put(CompositeDataConstants.SHORT_PROPERTIES, createTabularData(m, shortPropertyTabularType, Short.class));
            } catch (IOException e) {
                rc.put(CompositeDataConstants.SHORT_PROPERTIES, new TabularDataSupport(shortPropertyTabularType));
            }
            try {
                rc.put(CompositeDataConstants.INT_PROPERTIES, createTabularData(m, intPropertyTabularType, Integer.class));
            } catch (IOException e) {
                rc.put(CompositeDataConstants.INT_PROPERTIES, new TabularDataSupport(intPropertyTabularType));
            }
            try {
                rc.put(CompositeDataConstants.LONG_PROPERTIES, createTabularData(m, longPropertyTabularType, Long.class));
            } catch (IOException e) {
                rc.put(CompositeDataConstants.LONG_PROPERTIES, new TabularDataSupport(longPropertyTabularType));
            }
            try {
                rc.put(CompositeDataConstants.FLOAT_PROPERTIES, createTabularData(m, floatPropertyTabularType, Float.class));
            } catch (IOException e) {
                rc.put(CompositeDataConstants.FLOAT_PROPERTIES, new TabularDataSupport(floatPropertyTabularType));
            }
            try {
                rc.put(CompositeDataConstants.DOUBLE_PROPERTIES, createTabularData(m, doublePropertyTabularType, Double.class));
            } catch (IOException e) {
                rc.put(CompositeDataConstants.DOUBLE_PROPERTIES, new TabularDataSupport(doublePropertyTabularType));
            }
            return rc;
        }

        protected String toString(Object value) {
            if (value == null) {
                return null;
            }
            return value.toString();
        }


        protected <T> TabularType createTabularType(Class<T> type, OpenType openType) throws OpenDataException {
            String typeName = "java.util.Map<java.lang.String, " + type.getName() + ">";
            String[] keyValue = new String[]{"key", "value"};
            OpenType[] openTypes = new OpenType[]{SimpleType.STRING, openType};
            CompositeType rowType = new CompositeType(typeName, typeName, keyValue, keyValue, openTypes);
            return new TabularType(typeName, typeName, rowType, new String[]{"key"});
        }

        protected TabularDataSupport createTabularData(ActiveMQMessage m, TabularType type, Class valueType) throws IOException, OpenDataException {
            TabularDataSupport answer = new TabularDataSupport(type);
            Set<Map.Entry<String,Object>> entries = m.getProperties().entrySet();
            for (Map.Entry<String, Object> entry : entries) {
                Object value = entry.getValue();
                if (valueType.isInstance(value)) {
                    CompositeDataSupport compositeData = createTabularRowValue(type, entry.getKey(), value);
                    answer.put(compositeData);
                }
            }
            return answer;
        }

        protected CompositeDataSupport createTabularRowValue(TabularType type, String key, Object value) throws OpenDataException {
            Map<String,Object> fields = new HashMap<String, Object>();
            fields.put("key", key);
            fields.put("value", value);
            return new CompositeDataSupport(type.getRowType(), fields);
        }
    }

    static class ByteMessageOpenTypeFactory extends MessageOpenTypeFactory {


        protected String getTypeName() {
            return ActiveMQBytesMessage.class.getName();
        }

        protected void init() throws OpenDataException {
            super.init();
            addItem(BODY_LENGTH, "Body length", SimpleType.LONG);
            addItem(BODY_PREVIEW, "Body preview", new ArrayType(1, SimpleType.BYTE));
        }

        public Map<String, Object> getFields(Object o) throws OpenDataException {
            ActiveMQBytesMessage m = (ActiveMQBytesMessage)o;
            Map<String, Object> rc = super.getFields(o);
            long length = 0;
            try {
                length = m.getBodyLength();
                rc.put(BODY_LENGTH, Long.valueOf(length));
            } catch (JMSException e) {
                rc.put(BODY_LENGTH, Long.valueOf(0));
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

                rc.put(BODY_PREVIEW, data);
            } catch (JMSException e) {
                rc.put(BODY_PREVIEW, new byte[] {});
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
            addItem(CONTENT_MAP, "Content map", SimpleType.STRING);
        }

        public Map<String, Object> getFields(Object o) throws OpenDataException {
            ActiveMQMapMessage m = (ActiveMQMapMessage)o;
            Map<String, Object> rc = super.getFields(o);
            try {
                rc.put(CONTENT_MAP, "" + m.getContentMap());
            } catch (JMSException e) {
                rc.put(CONTENT_MAP, "");
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
            addItem(MESSAGE_TEXT, MESSAGE_TEXT, SimpleType.STRING);
        }

        public Map<String, Object> getFields(Object o) throws OpenDataException {
            ActiveMQTextMessage m = (ActiveMQTextMessage)o;
            Map<String, Object> rc = super.getFields(o);
            try {
                rc.put(MESSAGE_TEXT, "" + m.getText());
            } catch (JMSException e) {
                rc.put(MESSAGE_TEXT, "");
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
