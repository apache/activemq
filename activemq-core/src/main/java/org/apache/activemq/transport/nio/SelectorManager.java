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
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * The SelectorManager will manage one Selector and the thread that checks the
 * selector.
 * 
 * We may need to consider running more than one thread to check the selector if
 * servicing the selector takes too long.
 * 
 * @version $Rev: 46019 $ $Date: 2004-09-14 05:56:06 -0400 (Tue, 14 Sep 2004) $
 */
public final class SelectorManager {

    public static final SelectorManager SINGLETON = new SelectorManager();

    private Executor selectorExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
        public Thread newThread(Runnable r) {
            Thread rc = new Thread(r);
            rc.setName("NIO Transport Thread");
            return rc;
        }
    });
    private Executor channelExecutor = selectorExecutor;
    private LinkedList<SelectorWorker> freeWorkers = new LinkedList<SelectorWorker>();
    private int maxChannelsPerWorker = 64;
    
    public static SelectorManager getInstance() {
        return SINGLETON;
    }

    public interface Listener {
        void onSelect(SelectorSelection selector);

        void onError(SelectorSelection selection, Throwable error);
    }


    public synchronized SelectorSelection register(SocketChannel socketChannel, Listener listener)
        throws IOException {

        SelectorSelection selection = null;
        while( selection == null ) {
            if (freeWorkers.size() > 0) {
                SelectorWorker worker = freeWorkers.getFirst();
                if( worker.isReleased() ) {
                    freeWorkers.remove(worker);
                } else {
                    worker.retain();
                    selection = new SelectorSelection(worker, socketChannel, listener);
                }
                
            } else {
                // Worker starts /w retain count of 1
                SelectorWorker worker = new SelectorWorker(this);
                freeWorkers.addFirst(worker);
                selection = new SelectorSelection(worker, socketChannel, listener);
            }
        }
        
        return selection;
    }

    synchronized void onWorkerFullEvent(SelectorWorker worker) {
        freeWorkers.remove(worker);
    }

    public synchronized void onWorkerEmptyEvent(SelectorWorker worker) {
        freeWorkers.remove(worker);
    }

    public synchronized void onWorkerNotFullEvent(SelectorWorker worker) {
        freeWorkers.add(worker);
    }

    public Executor getChannelExecutor() {
        return channelExecutor;
    }

    public void setChannelExecutor(Executor channelExecutor) {
        this.channelExecutor = channelExecutor;
    }

    public int getMaxChannelsPerWorker() {
        return maxChannelsPerWorker;
    }

    public void setMaxChannelsPerWorker(int maxChannelsPerWorker) {
        this.maxChannelsPerWorker = maxChannelsPerWorker;
    }

    public Executor getSelectorExecutor() {
        return selectorExecutor;
    }

    public void setSelectorExecutor(Executor selectorExecutor) {
        this.selectorExecutor = selectorExecutor;
    }

}
