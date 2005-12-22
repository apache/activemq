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
package org.activecluster.impl;

import java.util.Map;
import javax.jms.JMSException;
import org.activecluster.LocalNode;
import org.activecluster.Service;


/**
 * Default implementation of a local Node which has its
 * state replicated across the cluster
 *
 * @version $Revision: 1.3 $
 */
public class ReplicatedLocalNode extends NodeImpl implements LocalNode, Service {

    /**
     * 
     */
    private static final long serialVersionUID=4626381612145333540L;
    private StateService serviceStub;

    /**
     * Create ReplicatedLocalNode
     * @param name
     * @param destination
     * @param serviceStub
     */
    public ReplicatedLocalNode(String name,String destination, StateService serviceStub) {
        super(name,destination);
        this.serviceStub = serviceStub;
    }

    /**
     * Set the State of the local node
     * @param state 
     */
    public void setState(Map state) {
        super.setState(state);
        serviceStub.keepAlive(this);
    }

    /**
     * ping remote nodes
     *
     */
    public void pingRemoteNodes() {
        serviceStub.keepAlive(this);
    }

    /**
     * start (lifecycle)
     * @throws JMSException 
     */
    public void start() throws JMSException {
    }

    /**
     * stop (lifecycle)
     * @throws JMSException 
     */
    public void stop() throws JMSException {
    }
}
