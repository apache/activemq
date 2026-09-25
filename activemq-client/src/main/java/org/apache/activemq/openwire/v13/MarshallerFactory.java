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
package org.apache.activemq.openwire.v13;

import org.apache.activemq.openwire.DataStreamMarshaller;
import org.apache.activemq.openwire.OpenWireFormat;

/**
 * OpenWire v13 MarshallerFactory.
 *
 * <p>Delegates to the v12 marshaller set and replaces only the marshallers
 * that changed in v13: {@code ConsumerInfoMarshaller} (adds {@code shared}
 * and {@code durable} fields), {@code SubscriptionInfoMarshaller}
 * (adds {@code shared} field), {@code ExceptionResponseMarshaller}
 * (adds {@code errorCode} field), and the message marshallers
 * (add {@code deliveryTime} field).
 */
public class MarshallerFactory {

    static final private DataStreamMarshaller[] marshaller;

    static {
        DataStreamMarshaller[] v12 =
            org.apache.activemq.openwire.v12.MarshallerFactory.createMarshallerMap(null);
        marshaller = v12.clone();
        add(new ConsumerInfoMarshaller());
        add(new SubscriptionInfoMarshaller());
        add(new ExceptionResponseMarshaller());
        add(new ActiveMQMessageMarshaller());
        add(new ActiveMQTextMessageMarshaller());
        add(new ActiveMQBytesMessageMarshaller());
        add(new ActiveMQMapMessageMarshaller());
        add(new ActiveMQStreamMessageMarshaller());
        add(new ActiveMQObjectMessageMarshaller());
        add(new ActiveMQBlobMessageMarshaller());
    }

    static private void add(DataStreamMarshaller dsm) {
        marshaller[dsm.getDataStructureType() & 0xFF] = dsm;
    }

    static public DataStreamMarshaller[] createMarshallerMap(OpenWireFormat wireFormat) {
        return marshaller;
    }
}
