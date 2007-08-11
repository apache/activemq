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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.command.BaseCommand;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.store.MessageStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Stores a messages/acknowledgements for a transaction
 * 
 * @version $Revision: 1.4 $
 */
class KahaTransaction {
    protected List<TxCommand> list = new ArrayList<TxCommand>();

    void add(KahaMessageStore store, BaseCommand command) {
        TxCommand tx = new TxCommand();
        tx.setCommand(command);
        tx.setMessageStoreKey(store.getId());
        list.add(tx);
    }

    Message[] getMessages() {
        List<BaseCommand> result = new ArrayList<BaseCommand>();
        for (int i = 0; i < list.size(); i++) {
            TxCommand command = list.get(i);
            if (command.isAdd()) {
                result.add(command.getCommand());
            }
        }
        Message[] messages = new Message[result.size()];
        return result.toArray(messages);
    }

    MessageAck[] getAcks() {
        List<BaseCommand> result = new ArrayList<BaseCommand>();
        for (int i = 0; i < list.size(); i++) {
            TxCommand command = list.get(i);
            if (command.isRemove()) {
                result.add(command.getCommand());
            }
        }
        MessageAck[] acks = new MessageAck[result.size()];
        return result.toArray(acks);
    }

    void prepare() {
    }

    void rollback() {
        list.clear();
    }

    /**
     * @throws IOException
     */
    void commit(KahaTransactionStore transactionStore) throws IOException {
        for (int i = 0; i < list.size(); i++) {
            TxCommand command = list.get(i);
            MessageStore ms = transactionStore.getStoreById(command.getMessageStoreKey());
            if (command.isAdd()) {
                ms.addMessage(null, (Message)command.getCommand());
            }
        }
        for (int i = 0; i < list.size(); i++) {
            TxCommand command = list.get(i);
            MessageStore ms = transactionStore.getStoreById(command.getMessageStoreKey());
            if (command.isRemove()) {
                ms.removeMessage(null, (MessageAck)command.getCommand());
            }
        }
    }

    List<TxCommand> getList() {
        return new ArrayList<TxCommand>(list);
    }

    void setList(List<TxCommand> list) {
        this.list = list;
    }
}
