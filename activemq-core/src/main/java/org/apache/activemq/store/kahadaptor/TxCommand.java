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
package org.apache.activemq.store.kahadaptor;

import org.apache.activemq.command.BaseCommand;
import org.apache.activemq.command.CommandTypes;


/**
 * Base class for  messages/acknowledgements for a transaction
 * 
 * @version $Revision: 1.4 $
 */
class TxCommand {
        protected Object messageStoreKey;
        protected BaseCommand command;

        /**
         * @return Returns the messageStoreKey.
         */
        public Object getMessageStoreKey(){
            return messageStoreKey;
        }

        /**
         * @param messageStoreKey The messageStoreKey to set.
         */
        public void setMessageStoreKey(Object messageStoreKey){
            this.messageStoreKey=messageStoreKey;
        }

        /**
         * @return Returns the command.
         */
        public BaseCommand getCommand(){
            return command;
        }

        /**
         * @param command The command to set.
         */
        public void setCommand(BaseCommand command){
            this.command=command;
        }
        
        /**
         * @return true if a Message command
         */
        public boolean isAdd(){
            return command != null && command.getDataStructureType() != CommandTypes.MESSAGE_ACK;
        }
        
        /**
         * @return true if a MessageAck command
         */
        public boolean isRemove(){
            return command != null && command.getDataStructureType() == CommandTypes.MESSAGE_ACK;
        }

  
    
}
