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
package org.apache.activemq.network.jms;


/**
 * Create an Inbound Topic Bridge
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class InboundTopicBridge extends TopicBridge{
       
    String inboundTopicName;
    String localTopicName;
    /**
     * Constructor that takes a foriegn destination as an argument
     * @param inboundTopicName
     */
    public  InboundTopicBridge(String  inboundTopicName){
        this.inboundTopicName = inboundTopicName;
        this.localTopicName = inboundTopicName;
    }
    
    /**
     * Default Contructor
     */
    public  InboundTopicBridge(){
    }

    /**
     * @return Returns the outboundTopicName.
     */
    public String getInboundTopicName(){
        return inboundTopicName;
    }

    /**
     * @param inboundTopicName 
     */
    public void setInboundTopicName(String inboundTopicName){
        this.inboundTopicName=inboundTopicName;
        if(this.localTopicName==null){
            this.localTopicName = inboundTopicName;
        }
    }

    /**
     * @return the localTopicName
     */
    public String getLocalTopicName(){
        return localTopicName;
    }

    /**
     * @param localTopicName the localTopicName to set
     */
    public void setLocalTopicName(String localTopicName){
        this.localTopicName=localTopicName;
    }
    
}
