/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
package org.apache.activemq.broker.region.virtual;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;

import java.util.Collection;
import java.util.Iterator;

/**
 * Represents a composite {@link Destination} where send()s are replicated to
 * each Destination instance.
 * 
 * @version $Revision$
 */
public class CompositeDestinationInterceptor extends DestinationFilter {

    private Collection forwardDestinations;
    private boolean forwardOnly;

    public CompositeDestinationInterceptor(Destination next, Collection forwardDestinations, boolean forwardOnly) {
        super(next);
        this.forwardDestinations = forwardDestinations;
        this.forwardOnly = forwardOnly;
    }

    public void send(ConnectionContext context, Message message) throws Exception {
        for (Iterator iter = forwardDestinations.iterator(); iter.hasNext();) {
            ActiveMQDestination destination = (ActiveMQDestination) iter.next();
            send(context, message, destination);
        }
        if (!forwardOnly) {
            super.send(context, message);
        }
    }
}
