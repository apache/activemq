/** 
 * 
 * Copyright 2005 LogicBlaze, Inc. (http://www.logicblaze.com)
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
 * 
 **/

package org.activecluster;

import javax.jms.JMSException;


/**
 * A Factory of Cluster instances
 * 
 * @version $Revision: 1.3 $
 */
public interface ClusterFactory {

    /**
     * Creates a new cluster connection using the given  local name and destination name
     * @param localName 
     * @param destination 
     *
     * @return Cluster
     * @throws JMSException
     */
    public Cluster createCluster(String localName,String destination) throws JMSException;


    /**
     * Creates a new cluster connection - generating the localName automatically
     * @param destination
     * @return
     * @throws JMSException
     */
    public Cluster createCluster(String destination) throws  JMSException;
}
