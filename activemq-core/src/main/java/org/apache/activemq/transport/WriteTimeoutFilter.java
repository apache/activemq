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
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.activemq.transport.tcp.TcpBufferedOutputStream;
import org.apache.activemq.transport.tcp.TcpTransport;

/**
 * This filter implements write timeouts for socket write operations.
 * When using blocking IO, the Java implementation doesn't have an explicit flag
 * to set a timeout, and can cause operations to block forever (or until the TCP stack implementation times out the retransmissions,
 * which is usually around 13-30 minutes).<br/>
 * To enable this transport, in the transport URI, simpley add<br/>
 * <code>transport.soWriteTimeout=<value in millis></code>.<br/>
 * For example (15 second timeout on write operations to the socket):</br>
 * <pre><code>
 * &lt;transportConnector 
 *     name=&quot;tcp1&quot; 
 *     uri=&quot;tcp://127.0.0.1:61616?transport.soTimeout=10000&amp;transport.soWriteTimeout=15000"
 * /&gt;
 * </code></pre><br/>
 * For example (enable default timeout on the socket):</br>
 * <pre><code>
 * &lt;transportConnector 
 *     name=&quot;tcp1&quot; 
 *     uri=&quot;tcp://127.0.0.1:61616?transport.soTimeout=10000&amp;transport.soWriteTimeout=15000"
 * /&gt;
 * </code></pre>
 * @author Filip Hanik
 *
 */
public class WriteTimeoutFilter extends TransportFilter {
	
    protected static ConcurrentLinkedQueue<WriteTimeoutFilter> writers = new ConcurrentLinkedQueue<WriteTimeoutFilter>();
    protected static AtomicInteger messageCounter = new AtomicInteger(0);
    protected static TimeoutThread timeoutThread = new TimeoutThread(); 
    
	protected long writeTimeout = -1;
	
    public WriteTimeoutFilter(Transport next) {
        super(next);
    }

    @Override
    public void oneway(Object command) throws IOException {
        try {
            registerWrite(this);
            super.oneway(command);
        } catch (IOException x) {
            deRegisterWrite(this,true,x);
            throw x;
        } finally {
            deRegisterWrite(this,false,null);
        }
    }
    
	public long getWriteTimeout() {
		return writeTimeout;
	}

	public void setWriteTimeout(long writeTimeout) {
		this.writeTimeout = writeTimeout;
	}
	
	protected TcpBufferedOutputStream getWriter() {
	    return next.narrow(TcpBufferedOutputStream.class);
	}
	
	protected static void registerWrite(WriteTimeoutFilter filter) {
	    writers.add(filter);
	}
	
    protected static boolean deRegisterWrite(WriteTimeoutFilter filter, boolean fail, IOException iox) {
        boolean result = writers.remove(filter); 
        if (result) {
            if (fail) {
                IOException ex = (iox!=null)?iox:new IOException("Forced write timeout for:"+filter.getNext().getRemoteAddress());
                filter.getTransportListener().onException(ex);
            }
        }
        return result;
    }
    
    @Override
    public void start() throws Exception {
        super.start();
    }
    
    @Override
    public void stop() throws Exception {
        super.stop();
    }
    
    protected static class TimeoutThread extends Thread {
        static AtomicInteger instance = new AtomicInteger(0);
        boolean run = true;
        public TimeoutThread() {
            setName("WriteTimeoutFilter-Timeout-"+instance.incrementAndGet());
            setDaemon(true);
            setPriority(Thread.MIN_PRIORITY);
            start();
        }

        
        public void run() {
            while (run) {
                if (!interrupted()) {
                    Iterator<WriteTimeoutFilter> filters = writers.iterator();
                    while (run && filters.hasNext()) { 
                        WriteTimeoutFilter filter = filters.next();
                        if (filter.getWriteTimeout()<=0) continue; //no timeout set
                        long writeStart = filter.getWriter().getWriteTimestamp();
                        long delta = (filter.getWriter().isWriting() && writeStart>0)?System.currentTimeMillis() - writeStart:-1;
                        if (delta>filter.getWriteTimeout()) {
                            WriteTimeoutFilter.deRegisterWrite(filter, true,null);
                        }//if timeout
                    }//while
                }//if interrupted
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException x) {
                    //do nothing
                }
            }
        }
    }

}
