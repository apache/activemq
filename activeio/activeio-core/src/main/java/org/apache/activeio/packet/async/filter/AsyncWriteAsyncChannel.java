/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activeio.packet.async.filter;

import edu.emory.mathcs.backport.java.util.concurrent.BlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.CountDownLatch;
import edu.emory.mathcs.backport.java.util.concurrent.Executor;
import edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingDeque;
import edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicInteger;

import org.apache.activeio.ChannelFactory;
import org.apache.activeio.packet.Packet;
import org.apache.activeio.packet.async.AsyncChannel;
import org.apache.activeio.packet.async.FilterAsyncChannel;

import java.io.IOException;
import java.io.InterruptedIOException;

public class AsyncWriteAsyncChannel extends FilterAsyncChannel {
    
    static public class ObjectDispatcherX implements Runnable {
        
        private final Executor executor;
        private final BlockingQueue queue;
        private final AtomicInteger size = new AtomicInteger(0);
        private final AsyncWriteAsyncChannel objectListener; 
        private long pollDelay=10;
        
        public ObjectDispatcherX(AsyncWriteAsyncChannel objectListener) {
            this(objectListener, 10);
        }

        public ObjectDispatcherX(AsyncWriteAsyncChannel objectListener, int queueSize) {
            this(objectListener, ChannelFactory.DEFAULT_EXECUTOR, new LinkedBlockingDeque(queueSize));
        }

        public ObjectDispatcherX(AsyncWriteAsyncChannel objectListener, Executor executor, BlockingQueue queue) {
            this.objectListener = objectListener;
            this.executor = executor;
            this.queue=queue;
        }
        
        public void add(Object o) throws InterruptedException {
            int t = size.incrementAndGet();
            queue.put(o);
            if( t==1 ) {
                executor.execute(this);
            }
        }

        synchronized public void run() {
            int t = size.get();
            while( t > 0 ) {
                int count=0;
                try {
                    Object o;
                    while( (o=queue.poll(pollDelay, TimeUnit.MILLISECONDS))!=null ) {
                        count++;
                        objectListener.onObject(o);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } finally {
                    t = size.addAndGet(-count);
                }
            }                
        }

    }
    static public class ObjectDispatcher {
        
        private final ThreadPoolExecutor executor;
        private final AsyncWriteAsyncChannel objectListener;
        
        public ObjectDispatcher(AsyncWriteAsyncChannel objectListener) {
            this(objectListener, 10);
        }

        public ObjectDispatcher(AsyncWriteAsyncChannel objectListener, int queueSize) {
            this.objectListener = objectListener;
            executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingDeque(queueSize));
            // executor.waitWhenBlocked();
        }
        
        public void add(final Object o) throws InterruptedException {
            executor.execute(new Runnable(){
                public void run() {
                    objectListener.onObject(o);
                }
            }); 
        }
    }

    private final ObjectDispatcher dispatcher;
    private static final Object FLUSH_COMMAND = new Object();
    
    public AsyncWriteAsyncChannel(AsyncChannel next) {
        this(next, 10);
    }
    
    public AsyncWriteAsyncChannel(AsyncChannel next, int queueSize) {
        super(next);
        this.dispatcher = new ObjectDispatcher(this, queueSize);
    }

    public void onObject(Object o) {
        try {
            if( o == FLUSH_COMMAND ) {
                next.flush();
                return;
            }
            if( o.getClass() == CountDownLatch.class ) {
                next.flush();
                ((CountDownLatch)o).countDown();
                return;
            }
            next.write((Packet)o);
        } catch (IOException e) {
            channelListener.onPacketError(e);
        }
    }
            
    public void write(Packet packet) throws IOException {
        try {
            dispatcher.add(packet);
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }
    }

    public void flush() throws IOException {
        flush(NO_WAIT_TIMEOUT);
    }
    
    public void stop() throws IOException {
        flush(WAIT_FOREVER_TIMEOUT);        
    }
    

    /**
     * @param timeout
     * @throws InterruptedIOException
     */
    private void flush(long timeout) throws InterruptedIOException {
        try {
            if( timeout == NO_WAIT_TIMEOUT ) {
                dispatcher.add(FLUSH_COMMAND);
            } else if( timeout == WAIT_FOREVER_TIMEOUT ) {
                CountDownLatch l = new CountDownLatch(1);
                dispatcher.add(l);
                l.await();
            } else {
                CountDownLatch l = new CountDownLatch(1);
                dispatcher.add(l);
                l.await(timeout, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }
    }
}
