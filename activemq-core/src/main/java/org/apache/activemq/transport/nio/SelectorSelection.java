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
package org.apache.activemq.transport.nio;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import org.apache.activemq.transport.nio.SelectorManager.Listener;

/**
 * @author chirino
 */
public final class SelectorSelection {

    private final SelectorWorker worker;
    private final SelectionKey key;
    private final Listener listener;
    private int interest;

    public SelectorSelection(SelectorWorker worker, SocketChannel socketChannel, Listener listener) throws ClosedChannelException {
        this.worker = worker;
        this.listener = listener;
        
        // Lock when mutating state of the selector
        worker.lock();
        
        try {
            this.key = socketChannel.register(worker.selector, 0, this);
            worker.incrementUseCounter();
        } finally {
            worker.unlock();
        }
    }

    public void setInterestOps(int ops) {
        interest = ops;
    }

    public void enable() {
        key.interestOps(interest);
        worker.selector.wakeup();
    }

    public void disable() {
        if (key.isValid()) {
            key.interestOps(0);
        }
    }

    public void close() {
        worker.decrementUseCounter();
        
        // Lock when mutating state of the selector
        worker.lock();
        try {
            key.cancel();
            if (!worker.isRunning()) {
                worker.close();
            }
        } catch (IOException e) {
        } finally {
            worker.unlock();
        }
    }

    public void onSelect() {
        listener.onSelect(this);
    }

    public void onError(Throwable e) {
        listener.onError(this, e);
    }

}
