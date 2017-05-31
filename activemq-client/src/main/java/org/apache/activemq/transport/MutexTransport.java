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
package org.apache.activemq.transport;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread safe Transport Filter that serializes calls to and from the Transport Stack.
 */
public class MutexTransport extends TransportFilter {

    private final ReentrantLock writeLock = new ReentrantLock();
    private boolean syncOnCommand;

    public MutexTransport(Transport next) {
        super(next);
        this.syncOnCommand = false;
    }

    public MutexTransport(Transport next, boolean syncOnCommand) {
        super(next);
        this.syncOnCommand = syncOnCommand;
    }

    @Override
    public void onCommand(Object command) {
        if (syncOnCommand) {
            writeLock.lock();
            try {
                transportListener.onCommand(command);
            } finally {
                writeLock.unlock();
            }
        } else {
            transportListener.onCommand(command);
        }
    }

    @Override
    public FutureResponse asyncRequest(Object command, ResponseCallback responseCallback) throws IOException {
        writeLock.lock();
        try {
            return next.asyncRequest(command, null);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void oneway(Object command) throws IOException {
        writeLock.lock();
        try {
            next.oneway(command);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Object request(Object command) throws IOException {
        writeLock.lock();
        try {
            return next.request(command);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Object request(Object command, int timeout) throws IOException {
        writeLock.lock();
        try {
            return next.request(command, timeout);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public String toString() {
        return next.toString();
    }

    public boolean isSyncOnCommand() {
        return syncOnCommand;
    }

    public void setSyncOnCommand(boolean syncOnCommand) {
        this.syncOnCommand = syncOnCommand;
    }
}
