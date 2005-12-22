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
package org.activemq.transport.stomp;

import java.io.DataInput;
import java.io.IOException;
import java.util.Properties;

import org.activemq.command.ActiveMQDestination;
import org.activemq.command.ConsumerInfo;
import org.activemq.util.IntrospectionSupport;

class Subscribe implements StompCommand {
    private HeaderParser headerParser = new HeaderParser();
    private StompWireFormat format;

    Subscribe(StompWireFormat format) {
        this.format = format;
    }

    public CommandEnvelope build(String commandLine, DataInput in) throws IOException {
        Properties headers = headerParser.parse(in);
        
        String subscriptionId = headers.getProperty(Stomp.Headers.Subscribe.ID);
        String destination = headers.getProperty(Stomp.Headers.Subscribe.DESTINATION);
        
        ActiveMQDestination actual_dest = DestinationNamer.convert(destination);
        ConsumerInfo ci = new ConsumerInfo(format.createConsumerId());
        ci.setPrefetchSize(1000);
        ci.setDispatchAsync(true);

        IntrospectionSupport.setProperties(ci, headers, "activemq:");
        
        ci.setDestination(DestinationNamer.convert(destination));
        
        while (in.readByte() != 0) {
        }
        
        Subscription s = new Subscription(format, subscriptionId, ci);
        s.setDestination(actual_dest);
        String ack_mode_key = headers.getProperty(Stomp.Headers.Subscribe.ACK_MODE);
        if (ack_mode_key != null && ack_mode_key.equals(Stomp.Headers.Subscribe.AckModeValues.CLIENT)) {
            s.setAckMode(Subscription.CLIENT_ACK);
        }

        format.addSubscription(s);
        return new CommandEnvelope(ci, headers);
    }
}
