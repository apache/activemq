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
package org.apache.activemq.systest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * 
 * @version $Revision: 1.1 $
 */
public abstract class ScenarioSupport implements Scenario {
    private static final Log log = LogFactory.getLog(ScenarioSupport.class);

    private LinkedList agents = new LinkedList();

    // Helper methods
    // -------------------------------------------------------------------------

    /**
     * Starts the given agent and adds it to the list of agents to stop when
     * the test is complete
     */
    public void start(Agent agent) throws Exception {
        agent.start();
        agents.addFirst(agent);
    }

    /**
     * Stops all the added agents
     */
    public void stop() throws Exception {
        AgentStopper stopper = new AgentStopper();
        stop(stopper);
        stopper.throwFirstException();
    }

    public void stop(AgentStopper stopper) {
        for (Iterator iter = agents.iterator(); iter.hasNext();) {
            Agent agent = (Agent) iter.next();
            log.info("Stopping agent: " + agent);
            stopper.stop(agent);
        }
    }

    
}
