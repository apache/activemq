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
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class SelectorWorker implements Runnable {

    private static final AtomicInteger NEXT_ID = new AtomicInteger();

    final SelectorManager manager;
    final Selector selector;
    final int id = NEXT_ID.getAndIncrement();
    final AtomicInteger useCounter = new AtomicInteger();
    private final int maxChannelsPerWorker;

    public SelectorWorker(SelectorManager manager) throws IOException {
        this.manager = manager;
        selector = Selector.open();
        maxChannelsPerWorker = manager.getMaxChannelsPerWorker();
    }

    void incrementUseCounter() {
        int use = useCounter.getAndIncrement();
        if (use == 0) {
            manager.getSelectorExecutor().execute(this);
        } else if (use + 1 == maxChannelsPerWorker) {
            manager.onWorkerFullEvent(this);
        }
    }

    void decrementUseCounter() {
        int use = useCounter.getAndDecrement();
        if (use == 1) {
            manager.onWorkerEmptyEvent(this);
        } else if (use == maxChannelsPerWorker) {
            manager.onWorkerNotFullEvent(this);
        }
    }

    boolean isRunning() {
        return useCounter.get() != 0;
    }

    public void run() {

        String origName = Thread.currentThread().getName();
        try {
            Thread.currentThread().setName("Selector Worker: " + id);
            while (isRunning()) {

                int count = selector.select(10);
                if (count == 0) {
                    continue;
                }

                if (!isRunning()) {
                    return;
                }

                // Get a java.util.Set containing the SelectionKey objects
                // for all channels that are ready for I/O.
                Set keys = selector.selectedKeys();

                for (Iterator i = keys.iterator(); i.hasNext();) {
                    final SelectionKey key = (SelectionKey)i.next();
                    i.remove();

                    final SelectorSelection s = (SelectorSelection)key.attachment();
                    try {
                        s.disable();

                        // Kick off another thread to find newly selected keys
                        // while we process the
                        // currently selected keys
                        manager.getChannelExecutor().execute(new Runnable() {
                            public void run() {
                                try {
                                    s.onSelect();
                                    s.enable();
                                } catch (Throwable e) {
                                    s.onError(e);
                                }
                            }
                        });

                    } catch (Throwable e) {
                        s.onError(e);
                    }

                }

            }
        } catch (IOException e) {

            // Don't accept any more slections
            manager.onWorkerEmptyEvent(this);

            // Notify all the selections that the error occurred.
            Set keys = selector.keys();
            for (Iterator i = keys.iterator(); i.hasNext();) {
                SelectionKey key = (SelectionKey)i.next();
                SelectorSelection s = (SelectorSelection)key.attachment();
                s.onError(e);
            }

        } finally {
            Thread.currentThread().setName(origName);
        }
    }
}
