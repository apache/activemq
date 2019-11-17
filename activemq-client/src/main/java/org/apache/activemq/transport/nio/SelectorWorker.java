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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectorWorker implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(SelectorWorker.class);

    private static final AtomicInteger NEXT_ID = new AtomicInteger();

    final SelectorManager manager;
    final Selector selector;
    final int id = NEXT_ID.getAndIncrement();
    private final int maxChannelsPerWorker;

    final AtomicInteger retainCounter = new AtomicInteger(1);
    private final ConcurrentLinkedQueue<Runnable> ioTasks = new ConcurrentLinkedQueue<Runnable>();

    public SelectorWorker(SelectorManager manager) throws IOException {
        this.manager = manager;
        selector = Selector.open();
        maxChannelsPerWorker = manager.getMaxChannelsPerWorker();
        manager.getSelectorExecutor().execute(this);
    }

    void retain() {
        if (retainCounter.incrementAndGet() == maxChannelsPerWorker) {
            manager.onWorkerFullEvent(this);
        }
    }

    void release() {
        int use = retainCounter.decrementAndGet();
        if (use == 0) {
            manager.onWorkerEmptyEvent(this);
        } else if (use == maxChannelsPerWorker - 1) {
            manager.onWorkerNotFullEvent(this);
        }
    }

    boolean isReleased() {
        return retainCounter.get() == 0;
    }

    public void addIoTask(Runnable work) {
        ioTasks.add(work);
        selector.wakeup();
    }

    private void processIoTasks() {
        Runnable task;
        while ((task = ioTasks.poll()) != null) {
            try {
                task.run();
            } catch (Throwable e) {
                LOG.debug(e.getMessage(), e);
            }
        }
    }

    @Override
    public void run() {

        String origName = Thread.currentThread().getName();
        try {
            Thread.currentThread().setName("Selector Worker: " + id);
            while (!isReleased()) {

                processIoTasks();

                int count = selector.select(10);

                if (count == 0) {
                    continue;
                }

                // Get a java.util.Set containing the SelectionKey objects
                // for all channels that are ready for I/O.
                Set<SelectionKey> keys = selector.selectedKeys();

                for (Iterator<SelectionKey> i = keys.iterator(); i.hasNext();) {
                    final SelectionKey key = i.next();
                    i.remove();

                    final SelectorSelection s = (SelectorSelection) key.attachment();
                    try {
                        if (key.isValid()) {
                            key.interestOps(0);
                        }

                        // Kick off another thread to find newly selected keys
                        // while we process the
                        // currently selected keys
                        manager.getChannelExecutor().execute(new Runnable() {
                            @Override
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
        } catch (Throwable e) {
            // Notify all the selections that the error occurred.
            Set<SelectionKey> keys = selector.keys();
            for (Iterator<SelectionKey> i = keys.iterator(); i.hasNext();) {
                SelectionKey key = i.next();
                SelectorSelection s = (SelectorSelection) key.attachment();
                s.onError(e);
            }
        } finally {
            try {
                manager.onWorkerEmptyEvent(this);
                selector.close();
            } catch (IOException ignore) {
                LOG.debug(ignore.getMessage(), ignore);
            }
            Thread.currentThread().setName(origName);
        }
    }
}
