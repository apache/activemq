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
package org.apache.activemq.transport.stomp;

import org.apache.activemq.command.ActiveMQDestination;

import java.io.DataInput;
import java.io.IOException;
import java.net.ProtocolException;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

public class Unsubscribe implements StompCommand {
    private static final HeaderParser parser = new HeaderParser();
    private final StompWireFormat format;

    Unsubscribe(StompWireFormat format) {
        this.format = format;
    }

    public CommandEnvelope build(String commandLine, DataInput in) throws IOException {
        Properties headers = parser.parse(in);
        while (in.readByte() == 0) {
        }

        String subscriptionId = headers.getProperty(Stomp.Headers.Unsubscribe.ID);
        String destination = headers.getProperty(Stomp.Headers.Unsubscribe.DESTINATION);


        if( subscriptionId!=null ) {
            Subscription s = format.getSubcription(subscriptionId);
            format.removeSubscription(s);
            return new CommandEnvelope(s.close(), headers);
        }
        
        ActiveMQDestination d = DestinationNamer.convert(destination);
        Set subs = format.getSubcriptions(d);
        for (Iterator iter = subs.iterator(); iter.hasNext();) {
            Subscription s = (Subscription) iter.next();
            format.removeSubscription(s);
            return new CommandEnvelope(s.close(), headers);
        }
        
        throw new ProtocolException("Unexpected UNSUBSCRIBE received.");

    }
}
