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

import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.transport.nio.SelectorManager.Listener;

/**
 *
 */
public final class SelectorSelection {

    private final SelectorWorker worker;
    private final Listener listener;
    private int interest;
    private SelectionKey key;
    private final AtomicBoolean closed = new AtomicBoolean();

    public SelectorSelection(final SelectorWorker worker, final AbstractSelectableChannel selectable, Listener listener) throws ClosedChannelException {
        this.worker = worker;
        this.listener = listener;
        worker.addIoTask(new Runnable() {
            @Override
            public void run() {
                try {
                    SelectorSelection.this.key = selectable.register(worker.selector, 0, SelectorSelection.this);
                } catch (Exception e) {
                    onError(e);
                }
            }
        });
    }

    public void setInterestOps(int ops) {
        interest = ops;
    }

    public void enable() {
        worker.addIoTask(new Runnable() {
            @Override
            public void run() {
                try {
                    key.interestOps(interest);
                } catch (CancelledKeyException e) {
                }
            }
        });
    }

    public void disable() {
        worker.addIoTask(new Runnable() {
            @Override
            public void run() {
                try {
                    key.interestOps(0);
                } catch (CancelledKeyException e) {
                }
            }
        });
    }

    public void close() {
        if (closed.compareAndSet(false, true)) {
            worker.addIoTask(new Runnable() {
                @Override
                public void run() {
                    try {
                        key.cancel();
                    } catch (CancelledKeyException e) {
                    } finally {
                        worker.release();
                    }
                }
            });
        }
    }

    public void onSelect() {
        listener.onSelect(this);
    }

    public void onError(Throwable e) {
        listener.onError(this, e);
    }
}
