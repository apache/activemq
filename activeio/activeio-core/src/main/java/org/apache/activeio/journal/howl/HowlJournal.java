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
package org.apache.activeio.journal.howl;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.activeio.journal.InvalidRecordLocationException;
import org.apache.activeio.journal.Journal;
import org.apache.activeio.journal.JournalEventListener;
import org.apache.activeio.journal.RecordLocation;
import org.apache.activeio.packet.ByteArrayPacket;
import org.apache.activeio.packet.Packet;
import org.objectweb.howl.log.Configuration;
import org.objectweb.howl.log.InvalidFileSetException;
import org.objectweb.howl.log.InvalidLogBufferException;
import org.objectweb.howl.log.InvalidLogKeyException;
import org.objectweb.howl.log.LogConfigurationException;
import org.objectweb.howl.log.LogEventListener;
import org.objectweb.howl.log.LogRecord;
import org.objectweb.howl.log.Logger;

/**
 * An implementation of the Journal interface using a HOWL logger.  This is is a thin
 * wrapper around a HOWL logger.
 * 
 * This implementation can be used to write records but not to retreive them
 * yet. Once the HOWL logger implements the methods needed to retreive
 * previously stored records, this class can be completed.
 * 
 * @version $Revision: 1.2 $
 */
public class HowlJournal implements Journal {

	private final Logger logger;

	private RecordLocation lastMark;

	public HowlJournal(Configuration configuration)
			throws InvalidFileSetException, LogConfigurationException,
			InvalidLogBufferException, ClassNotFoundException, IOException,
			InterruptedException {
		this.logger = new Logger(configuration);
		this.logger.open();
		lastMark = new LongRecordLocation(logger.getActiveMark());
	}

	/**
	 * @see org.apache.activeio.journal.Journal#write(byte[], boolean)
	 */
	public RecordLocation write(Packet packet, boolean sync) throws IOException {
		try {
			return new LongRecordLocation(logger.put(packet.sliceAsBytes(), sync));
		} catch (InterruptedException e) {
			throw (InterruptedIOException) new InterruptedIOException()
					.initCause(e);
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw (IOException) new IOException("Journal write failed: " + e)
					.initCause(e);
		}
	}

	/**
	 * @see org.apache.activeio.journal.Journal#setMark(org.codehaus.activemq.journal.RecordLocation, boolean)
	 */
	public void setMark(RecordLocation recordLocator, boolean force)
			throws InvalidRecordLocationException, IOException {
		try {
			long location = toLong(recordLocator);
			logger.mark(location, force);
			lastMark = recordLocator;

		} catch (InterruptedException e) {
			throw (InterruptedIOException) new InterruptedIOException()
					.initCause(e);
		} catch (IOException e) {
			throw e;
		} catch (InvalidLogKeyException e) {
			throw new InvalidRecordLocationException(e.getMessage(), e);
		} catch (Exception e) {
			throw (IOException) new IOException("Journal write failed: " + e)
					.initCause(e);
		}
	}
	
	/**
     * @param recordLocator
     * @return
     * @throws InvalidRecordLocationException
     */
    private long toLong(RecordLocation recordLocator) throws InvalidRecordLocationException {
        if (recordLocator == null
        		|| recordLocator.getClass() != LongRecordLocation.class)
        	throw new InvalidRecordLocationException();

        long location = ((LongRecordLocation) recordLocator)
        		.getLongLocation();
        return location;
    }

    /**
	 * @see org.apache.activeio.journal.Journal#getMark()
	 */
	public RecordLocation getMark() {
		return lastMark;
	}

	/**
	 * @see org.apache.activeio.journal.Journal#close()
	 */
	public void close() throws IOException {
		try {
			logger.close();
		} catch (IOException e) {
			throw e;
		} catch (InterruptedException e) {
			throw (InterruptedIOException) new InterruptedIOException()
					.initCause(e);
		} catch (Exception e) {
			throw (IOException) new IOException("Journal close failed: " + e)
					.initCause(e);
		}
	}

	/**
	 * @see org.apache.activeio.journal.Journal#setJournalEventListener(org.codehaus.activemq.journal.JournalEventListener)
	 */
	public void setJournalEventListener(final JournalEventListener eventListener) {
		logger.setLogEventListener(new LogEventListener() {
			public void logOverflowNotification(long key) {
				eventListener.overflowNotification(new LongRecordLocation(key));
			}
		});
	}

	/**
	 * @see org.apache.activeio.journal.Journal#getNextRecordLocation(org.codehaus.activemq.journal.RecordLocation)
	 */
	public RecordLocation getNextRecordLocation(RecordLocation lastLocation)
			throws InvalidRecordLocationException {
	    
	    if( lastLocation ==null ) {
	        if( this.lastMark !=null ) {
	            lastLocation = lastMark;
	        } else {
	            return null;
	        }
	    }
	    
	    try {
	        while(true) {
	            LogRecord record = logger.get(null, toLong(lastLocation));
		        // I assume getNext will return null if there is no next record. 
	            LogRecord next = logger.getNext(record);
	            if( next==null || next.length == 0 )
	                return null;
	            lastLocation = new LongRecordLocation(next.key);
	            if( !next.isCTRL() )
	                return lastLocation;
	        }
		} catch (Exception e) {
			throw (InvalidRecordLocationException)new InvalidRecordLocationException().initCause(e);
        }
		
	}

	/**
	 * @see org.apache.activeio.journal.Journal#read(org.codehaus.activemq.journal.RecordLocation)
	 */
	public Packet read(RecordLocation location)
			throws InvalidRecordLocationException, IOException {
	    
	    try {
            LogRecord record = logger.get(null, toLong(location));
            return new ByteArrayPacket(record.data);            
		} catch (InvalidLogKeyException e) {
			throw new InvalidRecordLocationException(e.getMessage(), e);
		} catch (Exception e) {
			throw (IOException) new IOException("Journal write failed: " + e)
					.initCause(e);
		}
		
	}

}