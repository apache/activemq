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
package org.apache.activemq.kaha.impl.data;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.StoreLocation;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.util.DataByteArrayOutputStream;

import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;

/**
 * Optimized Store writer that uses an async thread do batched writes to 
 * the datafile.
 * 
 * @version $Revision: 1.1.1.1 $
 */
final class AsyncDataFileWriter implements DataFileWriter {
//    static final Log log = LogFactory.getLog(AsyncDataFileWriter.class);
    
	private static final String SHUTDOWN_COMMAND = "SHUTDOWN";
	
	static public class WriteKey {
	    private final int file;
	    private final long offset;
	    private final int hash;

		public WriteKey(StoreLocation item){
	    	file = item.getFile();
	    	offset = item.getOffset();
	    	// TODO: see if we can build a better hash  
	    	hash = (int) (file  ^ offset);
	    }
	 
	    public int hashCode() {
	    	return hash;  
	    }
	    
	    public boolean equals(Object obj) {
	    	WriteKey di = (WriteKey)obj;
	    	return di.file == file && di.offset == offset;
	    }
	}

    public static class WriteCommand {
    	
		public final StoreLocation location;
		public final RandomAccessFile dataFile;
		public final byte[] data;
		public final CountDownLatch latch;

		public WriteCommand(StoreLocation location, RandomAccessFile dataFile, byte[] data, CountDownLatch latch) {
			this.location = location;
			this.dataFile = dataFile;
			this.data = data;
			this.latch = latch;
		}

		public String toString() {
			return "write: "+location+", latch = "+System.identityHashCode(latch);
		}
    }
    
    private DataManager dataManager;
    
    private final Object enqueueMutex = new Object();
    private final LinkedList queue = new LinkedList();
    
    // Maps WriteKey -> WriteCommand for all the writes that still have not landed on 
    // disk.
    private final ConcurrentHashMap inflightWrites = new ConcurrentHashMap();
    
    private final UsageManager usage = new UsageManager();     
    private CountDownLatch latchAssignedToNewWrites = new CountDownLatch(1);
    
    private boolean running;
    private boolean shutdown;
    private IOException firstAsyncException;
    private final CountDownLatch shutdownDone = new CountDownLatch(1);

    
    /**
     * Construct a Store writer
     * 
     * @param file
     */
    AsyncDataFileWriter(DataManager fileManager){
        this.dataManager=fileManager;
        this.usage.setLimit(1024*1024*8); // Allow about 8 megs of concurrent data to be queued up
    }
    
	public void force(final DataFile dataFile) throws IOException {
		try {
			CountDownLatch latch = null;
			
			synchronized( enqueueMutex ) {
				latch = (CountDownLatch) dataFile.getWriterData();
			}
			
			if( latch==null ) {
				return;
			}
			latch.await();
			
		} catch (InterruptedException e) {
			throw new InterruptedIOException();
		}
	}
	
    /**
     * @param marshaller
     * @param payload
     * @param type 
     * @return
     * @throws IOException
     */
    public synchronized DataItem storeItem(Marshaller marshaller, Object payload, byte type) throws IOException {
    	// We may need to slow down if we are pounding the async thread too 
    	// hard..
    	try {
			usage.waitForSpace();
		} catch (InterruptedException e) {
			throw new InterruptedIOException();
		}
        
        // Write the packet our internal buffer.
    	final DataByteArrayOutputStream buffer = new DataByteArrayOutputStream();
        buffer.position(DataManager.ITEM_HEAD_SIZE);
        marshaller.writePayload(payload,buffer);	
        final int size=buffer.size();
        int payloadSize=size-DataManager.ITEM_HEAD_SIZE;
        buffer.reset();
        buffer.writeByte(type);
        buffer.writeInt(payloadSize);
        final DataItem item=new DataItem();
        item.setSize(payloadSize);        
        usage.increaseUsage(size);

        // Locate datafile and enqueue into the executor in sychronized block so that 
        // writes get equeued onto the executor in order that they were assigned by 
        // the data manager (which is basically just appending)
    	WriteCommand write; 
        synchronized(enqueueMutex) {
            // Find the position where this item will land at.
	        final DataFile dataFile=dataManager.findSpaceForData(item);
	        dataManager.addInterestInFile(dataFile);
        	dataFile.setWriterData(latchAssignedToNewWrites);
        	write = new WriteCommand(item, dataFile.getRandomAccessFile(), buffer.getData(), latchAssignedToNewWrites);
	        enqueue(write);
        }
        
    	inflightWrites.put(new WriteKey(item), write);
        return item;
    }
    
    /**
     * 
     */
    public void updateItem(final DataItem item,Marshaller marshaller,Object payload,byte type) throws IOException{
        // We may need to slow down if we are pounding the async thread too
        // hard..
        try{
            usage.waitForSpace();
        }catch(InterruptedException e){
            throw new InterruptedIOException();
        }
        synchronized(enqueueMutex){
            // Write the packet our internal buffer.
            final DataByteArrayOutputStream buffer=new DataByteArrayOutputStream();
            buffer.position(DataManager.ITEM_HEAD_SIZE);
            marshaller.writePayload(payload,buffer);
            final int size=buffer.size();
            int payloadSize=size-DataManager.ITEM_HEAD_SIZE;
            buffer.reset();
            buffer.writeByte(type);
            buffer.writeInt(payloadSize);
            item.setSize(payloadSize);
            final DataFile dataFile=dataManager.getDataFile(item);
            usage.increaseUsage(size);
            WriteCommand write=new WriteCommand(item,dataFile.getRandomAccessFile(),buffer.getData(),
                    latchAssignedToNewWrites);
            // Equeue the write to an async thread.
            synchronized(enqueueMutex){
                dataFile.setWriterData(latchAssignedToNewWrites);
                enqueue(write);
            }
            inflightWrites.put(new WriteKey(item),write);
        }
    }

    private void enqueue(Object command) throws IOException {
    	
    	if( shutdown ) {
    		throw new IOException("Async Writter Thread Shutdown");
    	}
    	if( firstAsyncException !=null )
    		throw firstAsyncException;
    	
    	if( !running ) {
    		running=true;
    		Thread thread = new Thread() {
    			public void run() {
    				processQueue();
    			}
    		};
    		thread.setPriority(Thread.MAX_PRIORITY);
    		thread.setDaemon(true);
    		thread.setName("ActiveMQ Data File Writer");
    		thread.start();
    	}
  		queue.addLast(command);
  		enqueueMutex.notify();
    }
    
	private Object dequeue() {
		synchronized( enqueueMutex ) {
			while( queue.isEmpty() ) {
				inflightWrites.clear();
				try {
					enqueueMutex.wait();
				} catch (InterruptedException e) {
					return SHUTDOWN_COMMAND;
				}
			}
			return queue.removeFirst();
		}
	}
    
    public void close() throws IOException {
    	synchronized( enqueueMutex ) {
    		if( shutdown == false ) {
	    		shutdown = true;
	    		if( running ) {
	    			queue.add(SHUTDOWN_COMMAND);
	    	  		enqueueMutex.notify();
	    		} else {
	    			shutdownDone.countDown();
	    		}
    		}
    	}
    	
    	try {
			shutdownDone.await();
		} catch (InterruptedException e) {
			throw new InterruptedIOException();
		}
    	
    }

    boolean isShutdown() {
    	synchronized( enqueueMutex ) {
    		return shutdown;
    	}    	
    }
    
    /**
     * The async processing loop that writes to the data files and
     * does the force calls.  
     * 
     * Since the file sync() call is the slowest of all the operations, 
     * this algorithm tries to 'batch' or group together several file sync() requests 
     * into a single file sync() call. The batching is accomplished attaching the 
     * same CountDownLatch instance to every force request in a group.
     * 
     */
    private void processQueue() {
//    	log.debug("Async thread startup");
    	try {
    		CountDownLatch currentBatchLatch=null;
    		RandomAccessFile currentBatchDataFile=null;

	    	while( true ) {
	    		
	    		// Block till we get a command.
	    		Object o = dequeue();
//        		log.debug("Processing: "+o);
	    			    		
	        	if( o == SHUTDOWN_COMMAND ) {
	        		if( currentBatchLatch!=null ) {
	        			currentBatchDataFile.getFD().sync();
	        			currentBatchLatch.countDown();
	        		}
	        		break;
	        	} else if( o.getClass() == CountDownLatch.class ) {
		        	// The CountDownLatch is used as the end of batch indicator.	        		
	        		// Must match..  
	        		if( o == currentBatchLatch ) {
	        			currentBatchDataFile.getFD().sync();
	        			currentBatchLatch.countDown();
	        			currentBatchLatch=null;
	        			currentBatchDataFile=null;
	        		} else {
	        			new IOException("Got an out of sequence end of end of batch indicator.");
	        		}
	        		
	        	} else if( o.getClass() == WriteCommand.class ) {
	        		
        			WriteCommand write = (WriteCommand) o;

        			if( currentBatchDataFile == null )
        				currentBatchDataFile = write.dataFile;
        			
        			// We may need to prematurely sync if the batch
        			// if user is switching between data files.
        			if( currentBatchDataFile!=write.dataFile ) {
	        			currentBatchDataFile.getFD().sync();
	        			currentBatchDataFile = write.dataFile;
        			}
        			
        			// Write to the data..
        			int size = write.location.getSize()+DataManager.ITEM_HEAD_SIZE;
        			synchronized(write.dataFile) {
			        	write.dataFile.seek(write.location.getOffset());
			        	write.dataFile.write(write.data,0,size);
        			}
		        	inflightWrites.remove(new WriteKey(write.location));
		        	usage.decreaseUsage(size);		        	
		        	
	        		// Start of a batch..
	        		if( currentBatchLatch == null ) {
	        			currentBatchLatch = write.latch;

        	        	synchronized(enqueueMutex) {
        	        		// get the request threads to start using a new latch..
        	        		// write commands allready in the queue should have the 
        	        		// same latch assigned.
        	        		latchAssignedToNewWrites = new CountDownLatch(1);
        	        		if( !shutdown ) {
        	        			enqueue(currentBatchLatch);
        	        		}
        	        	}
        	        	
	        		} else if( currentBatchLatch!=write.latch ) { 
	        			// the latch on subsequent writes should match.
	        			new IOException("Got an out of sequence write");
	        		}
	        	}
	    	}
	    	
		} catch (IOException e) {
	    	synchronized( enqueueMutex ) {
	    		firstAsyncException = e;
	    	}
//			log.debug("Aync thread shutdown due to error: "+e,e);
		} finally {
//			log.debug("Aync thread shutdown");
    		shutdownDone.countDown();
    	}
    }

	public synchronized ConcurrentHashMap getInflightWrites() {
		return inflightWrites;
	}
        
}
