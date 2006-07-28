/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activeio.packet.async.nio;

import edu.emory.mathcs.backport.java.util.concurrent.Executor;
import edu.emory.mathcs.backport.java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.activeio.ChannelFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

/**
 * The SelectorManager will manage one Selector and the thread that checks the
 * selector.
 * 
 * We may need to consider running more than one thread to check the selector if
 * servicing the selector takes too long.
 * 
 * @version $Rev: 46019 $ $Date: 2004-09-14 05:56:06 -0400 (Tue, 14 Sep 2004) $
 */
final public class NIOAsyncChannelSelectorManager {

    static private Executor selectorExecutor = ChannelFactory.DEFAULT_EXECUTOR;
    static private Executor channelExecutor = ChannelFactory.DEFAULT_EXECUTOR;
    
    static private LinkedList freeManagers = new LinkedList();
    static private LinkedList fullManagers = new LinkedList();
    private static final int MAX_CHANNELS_PER_SELECTOR  = 50;
    
    static {
       String os = System.getProperty("os.name");
       if( os.startsWith("Linux") ) {
           channelExecutor = new ScheduledThreadPoolExecutor(1);
       }
    } 

    public static interface SelectorManagerListener {
        public void onSelect(SocketChannelAsyncChannelSelection selector);
    }

    final public class SocketChannelAsyncChannelSelection {
        
        private final SelectionKey key;
        private final SelectorManagerListener listener;
        private boolean closed;
        private int interest;

        private SocketChannelAsyncChannelSelection(SocketChannel socketChannel, SelectorManagerListener listener)
                throws ClosedChannelException {
            this.listener = listener;
            this.key = socketChannel.register(selector, 0, this);
            incrementUseCounter();
        }

        public void setInterestOps(int ops) {
            	if( closed ) 
            		return;
            	interest = ops;
             enable();
        }
        
        public void enable() {
            if( closed ) 
                return;
            key.interestOps(interest);
            selector.wakeup();
        }

        public void disable() {
            if( closed ) 
                return;
            key.interestOps(0);
        }

        public void close() {
        	if( closed ) 
        		return;
        	
            key.cancel();
            decrementUseCounter();
            selector.wakeup();
            closed=true;
        }
        
        public void onSelect() {
            if( !key.isValid() )
                return;
            listener.onSelect(this);
        }

        public boolean isWritable() {
            return key.isWritable();
        }

        public boolean isReadable() {
            return key.isReadable();
        }
    }

    public synchronized static SocketChannelAsyncChannelSelection register(
            SocketChannel socketChannel, SelectorManagerListener listener)
            throws IOException {

        NIOAsyncChannelSelectorManager manager = null;
        synchronized (freeManagers) {
            if (freeManagers.size() > 0)
                manager = (NIOAsyncChannelSelectorManager) freeManagers.getFirst();
            if (manager == null) {
                manager = new NIOAsyncChannelSelectorManager();
                freeManagers.addFirst(manager);
            }

            // That manager may have filled up.
            SocketChannelAsyncChannelSelection selection = manager.new SocketChannelAsyncChannelSelection(
                    socketChannel, listener);
            if (manager.useCounter >= MAX_CHANNELS_PER_SELECTOR) {
                freeManagers.removeFirst();
                fullManagers.addLast(manager);
            }
            return selection;
        }
    }

    public synchronized static void setSelectorExecutor(Executor executor) {
        NIOAsyncChannelSelectorManager.selectorExecutor = executor;
    }
    
    public synchronized static void setChannelExecutor(Executor executor) {
        NIOAsyncChannelSelectorManager.channelExecutor = executor;
    }

    private class SelectorWorker implements Runnable {
                
        public void run() {            
            
            String origName = Thread.currentThread().getName();
            try {
               Thread.currentThread().setName("Selector Worker: "+getId());
               while ( isRunning() ) {

                   int count = selector.select(10);
                   if (count == 0)
                       continue;                
                    if( !isRunning() )
                        return;

                    // Get a java.util.Set containing the SelectionKey objects
                    // for all channels that are ready for I/O.
                    Set keys = selector.selectedKeys();
    
                    for (Iterator i = keys.iterator(); i.hasNext();) {                        
                        final SelectionKey key = (SelectionKey) i.next();
                        i.remove();

                        if( !key.isValid() ) 
                            continue;
                        
                        final SocketChannelAsyncChannelSelection s = (SocketChannelAsyncChannelSelection) key.attachment();
                        s.disable();
                        
                        // Kick off another thread to find newly selected keys while we process the 
                        // currently selected keys                
                        channelExecutor.execute(new Runnable() {
                            public void run() {
                                try {
                                    s.onSelect();
                                    s.enable();
                                } catch ( Throwable e ) {
                                    System.err.println("ActiveIO unexpected error: ");
                                    e.printStackTrace(System.err);
                                }
                            }
                        });
                    }
                    
               }
            } catch (Throwable e) {
                System.err.println("Unexpected exception: " + e);
                e.printStackTrace();
            } finally {
                Thread.currentThread().setName(origName);
            }
        }
    }

    /**
     * The selector used to wait for non-blocking events.
     */
    private Selector selector;

    /**
     * How many SelectionKeys does the selector have active.
     */
    private int useCounter;
    private int id = getNextId();
    private static int nextId;

    private NIOAsyncChannelSelectorManager() throws IOException {
        selector = Selector.open();
    }
    
    synchronized private static int getNextId() {
        return nextId++;
    }

    private int getId() {
        return id ;
    }

    synchronized private void incrementUseCounter() {
        useCounter++;
        if (useCounter == 1) {
            selectorExecutor.execute(new SelectorWorker());
        }
    }

    synchronized private void decrementUseCounter() {
        useCounter--;
	 	synchronized(freeManagers) {	   	 		 
 	 		 if( useCounter == 0 ) {
  	 		 	freeManagers.remove(this);
  	 		 }    	 		 
 	 		 else if( useCounter < MAX_CHANNELS_PER_SELECTOR ) {
  	 		 	fullManagers.remove(this);
  	 		 	freeManagers.addLast(this);
  	 		 }     	 		 
	    }
    }

    synchronized private boolean isRunning() {
        return useCounter > 0;
    }
}
