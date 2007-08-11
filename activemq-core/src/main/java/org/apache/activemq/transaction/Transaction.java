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
package org.apache.activemq.transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import javax.transaction.xa.XAException;

import org.apache.activemq.command.TransactionId;

/**
 * Keeps track of all the actions the need to be done when a transaction does a
 * commit or rollback.
 * 
 * @version $Revision: 1.5 $
 */
public abstract class Transaction {

    public static final byte START_STATE = 0; // can go to: 1,2,3
    public static final byte IN_USE_STATE = 1; // can go to: 2,3
    public static final byte PREPARED_STATE = 2; // can go to: 3
    public static final byte FINISHED_STATE = 3;

    private ArrayList<Synchronization> synchronizations = new ArrayList<Synchronization>();
    private byte state = START_STATE;

    public byte getState() {
        return state;
    }

    public void setState(byte state) {
        this.state = state;
    }

    public void addSynchronization(Synchronization r) {
        synchronizations.add(r);
        if (state == START_STATE) {
            state = IN_USE_STATE;
        }
    }

    public void removeSynchronization(Synchronization r) {
        synchronizations.remove(r);
    }

    public void prePrepare() throws Exception {

        // Is it ok to call prepare now given the state of the
        // transaction?
        switch (state) {
        case START_STATE:
        case IN_USE_STATE:
            break;
        default:
            XAException xae = new XAException("Prepare cannot be called now.");
            xae.errorCode = XAException.XAER_PROTO;
            throw xae;
        }

        // // Run the prePrepareTasks
        // for (Iterator iter = prePrepareTasks.iterator(); iter.hasNext();) {
        // Callback r = (Callback) iter.next();
        // r.execute();
        // }
    }

    protected void fireAfterCommit() throws Exception {
        for (Iterator<Synchronization> iter = synchronizations.iterator(); iter.hasNext();) {
            Synchronization s = iter.next();
            s.afterCommit();
        }
    }

    public void fireAfterRollback() throws Exception {
        for (Iterator<Synchronization> iter = synchronizations.iterator(); iter.hasNext();) {
            Synchronization s = iter.next();
            s.afterRollback();
        }
    }

    public String toString() {
        return super.toString() + "[synchronizations=" + synchronizations + "]";
    }

    public abstract void commit(boolean onePhase) throws XAException, IOException;

    public abstract void rollback() throws XAException, IOException;

    public abstract int prepare() throws XAException, IOException;

    public abstract TransactionId getTransactionId();

    public boolean isPrepared() {
        return getState() == PREPARED_STATE;
    }
}
