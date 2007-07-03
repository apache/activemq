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
 */
package org.apache.activemq;

import java.util.LinkedHashMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.util.LRUCache;


/**
 * An auditor class for a Connection that looks for duplicates
 */
 class ConnectionAudit{

    private boolean checkForDuplicates;
    private LinkedHashMap<ActiveMQDestination,ActiveMQMessageAudit> queues=new LRUCache<ActiveMQDestination,ActiveMQMessageAudit>(
            1000);
    private LinkedHashMap<ActiveMQDispatcher,ActiveMQMessageAudit> dispatchers=new LRUCache<ActiveMQDispatcher,ActiveMQMessageAudit>(
            1000);

    synchronized void removeDispatcher(ActiveMQDispatcher dispatcher){
        dispatchers.remove(dispatcher);
    }

    synchronized boolean isDuplicate(ActiveMQDispatcher dispatcher,Message message){
        if(checkForDuplicates && message != null){
            ActiveMQDestination destination=message.getDestination();
            if(destination!=null){
                if(destination.isQueue()){
                    ActiveMQMessageAudit audit=queues.get(destination);
                    if(audit==null){
                        audit=new ActiveMQMessageAudit();
                        queues.put(destination,audit);
                    }
                    boolean result=audit.isDuplicateMessageReference(message);
                    return result;
                }
                ActiveMQMessageAudit audit=dispatchers.get(dispatcher);
                if(audit==null){
                    audit=new ActiveMQMessageAudit();
                    dispatchers.put(dispatcher,audit);
                }
                boolean result=audit.isDuplicateMessageReference(message);
                return result;
            }
        }
        return false;
    }

    protected void rollbackDuplicate(ActiveMQDispatcher dispatcher,Message message){
        if(checkForDuplicates && message != null){
            ActiveMQDestination destination=message.getDestination();
            if(destination!=null){
                if(destination.isQueue()){
                    ActiveMQMessageAudit audit=queues.get(destination);
                    if(audit!=null){
                        audit.rollbackMessageReference(message);
                    }
                }else{
                    ActiveMQMessageAudit audit=dispatchers.get(dispatcher);
                    if(audit!=null){
                        audit.rollbackMessageReference(message);
                    }
                }
            }
        }
    }

    /**
     * @return the checkForDuplicates
     */
    boolean isCheckForDuplicates(){
        return this.checkForDuplicates;
    }

    /**
     * @param checkForDuplicates the checkForDuplicates to set
     */
    void setCheckForDuplicates(boolean checkForDuplicates){
        this.checkForDuplicates=checkForDuplicates;
    }
}
