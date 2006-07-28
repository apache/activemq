/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

package org.apache.activecluster;

import javax.jms.Destination;
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
     * @param destinationName 
     *
     * @return Cluster
     * @throws JMSException
     */
    public Cluster createCluster(String localName,String destinationName) throws JMSException;
    
    /**
     * Creates a new cluster connection using the given  local name and destination name
     * @param localName 
     * @param destinationName 
     * @param marshaller 
     *
     * @return Cluster
     * @throws JMSException
     */
    public Cluster createCluster(String localName,String destinationName,DestinationMarshaller marshaller) throws JMSException;



    /**
     * Creates a new cluster connection - generating the localName automatically
     * @param destinationName
     * @return the Cluster
     * @throws JMSException
     */
    public Cluster createCluster(String destinationName) throws  JMSException;
    
    /**
     * Creates a new cluster connection using the given  local name and destination name
     * @param localName 
     * @param destination
     *
     * @return Cluster
     * @throws JMSException
     */
    public Cluster createCluster(String localName,Destination destination) throws JMSException;
    
    /**
     * Creates a new cluster connection using the given  local name and destination name
     * @param localName 
     * @param destination
     * @param marshaller 
     *
     * @return Cluster
     * @throws JMSException
     */
    public Cluster createCluster(String localName,Destination destination, DestinationMarshaller marshaller) throws JMSException;



    /**
     * Creates a new cluster connection - generating the localName automatically
     * @param destination
     * @return the Cluster
     * @throws JMSException
     */
    public Cluster createCluster(Destination destination) throws  JMSException;
}
