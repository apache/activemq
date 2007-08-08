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

package org.apache.activemq.openwire.v1;

import org.apache.activemq.openwire.DataStreamMarshaller;
import org.apache.activemq.openwire.OpenWireFormat;

/**
 * MarshallerFactory for Open Wire Format.
 *
 *
 * NOTE!: This file is auto generated - do not modify!
 *        if you need to make a change, please see the modify the groovy scripts in the
 *        under src/gram/script and then use maven openwire:generate to regenerate 
 *        this file.
 *
 * @version $Revision$
 */
public class MarshallerFactory {

    /**
     * Creates a Map of command type -> Marshallers
     */
    static final private DataStreamMarshaller marshaller[] = new DataStreamMarshaller[256];
    static {

        add(new LocalTransactionIdMarshaller());
        add(new PartialCommandMarshaller());
        add(new IntegerResponseMarshaller());
        add(new ActiveMQQueueMarshaller());
        add(new ActiveMQObjectMessageMarshaller());
        add(new ConnectionIdMarshaller());
        add(new ConnectionInfoMarshaller());
        add(new ProducerInfoMarshaller());
        add(new MessageDispatchNotificationMarshaller());
        add(new SessionInfoMarshaller());
        add(new TransactionInfoMarshaller());
        add(new ActiveMQStreamMessageMarshaller());
        add(new MessageAckMarshaller());
        add(new ProducerIdMarshaller());
        add(new MessageIdMarshaller());
        add(new ActiveMQTempQueueMarshaller());
        add(new RemoveSubscriptionInfoMarshaller());
        add(new SessionIdMarshaller());
        add(new DataArrayResponseMarshaller());
        add(new JournalQueueAckMarshaller());
        add(new ResponseMarshaller());
        add(new ConnectionErrorMarshaller());
        add(new ConsumerInfoMarshaller());
        add(new XATransactionIdMarshaller());
        add(new JournalTraceMarshaller());
        add(new ConsumerIdMarshaller());
        add(new ActiveMQTextMessageMarshaller());
        add(new SubscriptionInfoMarshaller());
        add(new JournalTransactionMarshaller());
        add(new ControlCommandMarshaller());
        add(new LastPartialCommandMarshaller());
        add(new NetworkBridgeFilterMarshaller());
        add(new ActiveMQBytesMessageMarshaller());
        add(new WireFormatInfoMarshaller());
        add(new ActiveMQTempTopicMarshaller());
        add(new DiscoveryEventMarshaller());
        add(new ReplayCommandMarshaller());
        add(new ActiveMQTopicMarshaller());
        add(new BrokerInfoMarshaller());
        add(new DestinationInfoMarshaller());
        add(new ShutdownInfoMarshaller());
        add(new DataResponseMarshaller());
        add(new ConnectionControlMarshaller());
        add(new KeepAliveInfoMarshaller());
        add(new FlushCommandMarshaller());
        add(new ConsumerControlMarshaller());
        add(new JournalTopicAckMarshaller());
        add(new BrokerIdMarshaller());
        add(new MessageDispatchMarshaller());
        add(new ActiveMQMapMessageMarshaller());
        add(new ActiveMQMessageMarshaller());
        add(new RemoveInfoMarshaller());
        add(new ExceptionResponseMarshaller());

	}

	static private void add(DataStreamMarshaller dsm) {
        marshaller[dsm.getDataStructureType()] = dsm;
    }
	
    static public DataStreamMarshaller[] createMarshallerMap(OpenWireFormat wireFormat) {
        return marshaller;
    }
}
