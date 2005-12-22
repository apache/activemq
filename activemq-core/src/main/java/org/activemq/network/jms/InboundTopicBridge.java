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
package org.activemq.network.jms;


/**
 * Create an Inbound Topic Bridge
 * 
 * @org.xbean.XBean
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class InboundTopicBridge extends TopicBridge{
       
    String inboundTopicName;
    /**
     * Constructor that takes a foriegn destination as an argument
     * @param inboundTopicName
     */
    public  InboundTopicBridge(String  inboundTopicName){
        this.inboundTopicName = inboundTopicName;
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
     * @param outboundTopicName The outboundTopicName to set.
     */
    public void setInboundTopicName(String inboundTopicName){
        this.inboundTopicName=inboundTopicName;
    }
    
}