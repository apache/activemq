/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

        add(new ActiveMQMessageMarshaller());
        add(new MessageIdMarshaller());
        add(new ControlCommandMarshaller());
        add(new FlushCommandMarshaller());
        add(new IntegerResponseMarshaller());
        add(new RemoveSubscriptionInfoMarshaller());
        add(new SubscriptionInfoMarshaller());
        add(new DataArrayResponseMarshaller());
        add(new ConnectionIdMarshaller());
        add(new BrokerInfoMarshaller());
        add(new JournalTraceMarshaller());
        add(new MessageDispatchMarshaller());
        add(new KeepAliveInfoMarshaller());
        add(new ActiveMQStreamMessageMarshaller());
        add(new JournalQueueAckMarshaller());
        add(new ActiveMQTempTopicMarshaller());
        add(new ProducerInfoMarshaller());
        add(new BrokerIdMarshaller());
        add(new MessageAckMarshaller());
        add(new ActiveMQBytesMessageMarshaller());
        add(new SessionInfoMarshaller());
        add(new ActiveMQTextMessageMarshaller());
        add(new ActiveMQMapMessageMarshaller());
        add(new ShutdownInfoMarshaller());
        add(new DataResponseMarshaller());
        add(new JournalTopicAckMarshaller());
        add(new DestinationInfoMarshaller());
        add(new XATransactionIdMarshaller());
        add(new ActiveMQObjectMessageMarshaller());
        add(new ConsumerIdMarshaller());
        add(new SessionIdMarshaller());
        add(new ConsumerInfoMarshaller());
        add(new ConnectionInfoMarshaller());
        add(new ActiveMQTopicMarshaller());
        add(new RedeliveryPolicyMarshaller());
        add(new ExceptionResponseMarshaller());
        add(new JournalTransactionMarshaller());
        add(new ProducerIdMarshaller());
        add(new ActiveMQQueueMarshaller());
        add(new ActiveMQTempQueueMarshaller());
        add(new TransactionInfoMarshaller());
        add(new ResponseMarshaller());
        add(new RemoveInfoMarshaller());
        add(new WireFormatInfoMarshaller());
        add(new LocalTransactionIdMarshaller());

	}

	static private void add(DataStreamMarshaller dsm) {
        marshaller[dsm.getDataStructureType()] = dsm;
    }
	
    static public DataStreamMarshaller[] createMarshallerMap(OpenWireFormat wireFormat) {
        return marshaller;
    }
}
