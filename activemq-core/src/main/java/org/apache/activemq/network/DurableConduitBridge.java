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
package org.apache.activemq.network;

import java.io.IOException;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.transport.Transport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Consolidates subscriptions
 * 
 * @version $Revision: 1.1 $
 */
public class DurableConduitBridge extends ConduitBridge{
    static final private Log log=LogFactory.getLog(DurableConduitBridge.class);
    /**
     * Constructor
     * @param localBroker
     * @param remoteBroker
     */
    public DurableConduitBridge(Transport localBroker,Transport remoteBroker){
        super(localBroker,remoteBroker);
    }
    
    /**
     * Subscriptions for these desitnations are always created
     * @throws IOException 
     *
     */
    protected void setupStaticDestinations() throws IOException{
        super.setupStaticDestinations();
        ActiveMQDestination[] dests=durableDestinations;
        if(dests!=null){
            for(int i=0;i<dests.length;i++){
                ActiveMQDestination dest=dests[i];
                if(isPermissableDestination(dest)){
                    DemandSubscription sub=createDemandSubscription(dest);
                    addSubscription(sub);
                    if(log.isTraceEnabled())
                        log.trace("Forwarding messages for durable destination: "+dest);
                }
            }
        }
    }

}
