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
package org.apache.activemq.store.journal;

import java.io.IOException;

import org.apache.activemq.store.journal.packet.Packet;

/**
 * A Journal is a record logging Interface that can be used to implement 
 * a transaction log.  
 * 
 * 
 * This interface was largely extracted out of the HOWL project to allow 
 * ActiveMQ to switch between different Journal implementations verry easily. 
 * 
 * @version $Revision: 1.1 $
 */
public interface Journal {

	/**
	 * Writes a {@see Packet} of  data to the journal.  If <code>sync</code>
	 * is true, then this call blocks until the data has landed on the physical 
	 * disk.  Otherwise, this enqueues the write request and returns.
	 * 
	 * @param record - the data to be written to disk.
	 * @param sync - If this call should block until the data lands on disk.
	 * 
	 * @return RecordLocation the location where the data will be written to on disk.
	 * 
	 * @throws IOException if the write failed.
	 * @throws IllegalStateException if the journal is closed.
	 */
	public RecordLocation write(Packet packet, boolean sync) throws IOException, IllegalStateException;

	/**
	 * Reads a previously written record from the journal. 
	 *  
	 * @param location is where to read the record from.
	 * 
	 * @return the data previously written at the <code>location</code>.
	 * 
	 * @throws InvalidRecordLocationException if <code>location</code> parameter is out of range.  
	 *         It cannot be a location that is before the current mark. 
	 * @throws IOException if the record could not be read.
	 * @throws IllegalStateException if the journal is closed.
	 */
	public Packet read(RecordLocation location) throws InvalidRecordLocationException, IOException, IllegalStateException;

	/**
	 * Informs the journal that all the journal space up to the <code>location</code> is no longer
	 * needed and can be reclaimed for reuse.
	 * 
	 * @param location the location of the record to mark.  All record locations before the marked 
	 * location will no longger be vaild. 
	 * 
	 * @param sync if this call should block until the mark is set on the journal.
	 * 
	 * @throws InvalidRecordLocationException if <code>location</code> parameter is out of range.  
	 *         It cannot be a location that is before the current mark. 
	 * @throws IOException if the record could not be read.
	 * @throws IllegalStateException if the journal is closed.
	 */
	public abstract void setMark(RecordLocation location, boolean sync)
			throws InvalidRecordLocationException, IOException, IllegalStateException;
	
	/**
	 * Obtains the mark that was set in the Journal.
	 * 
	 * @see read(RecordLocation location);
	 * @return the mark that was set in the Journal.
	 * @throws IllegalStateException if the journal is closed.
	 */
	public RecordLocation getMark() throws IllegalStateException;


	/**
	 * Close the Journal.  
	 * This is blocking operation that waits for any pending put opperations to be forced to disk.
	 * Once the Journal is closed, all other methods of the journal should throw IllegalStateException.
	 * 
	 * @throws IOException if an error occurs while the journal is being closed.
	 */
	public abstract void close() throws IOException;

	/**
	 * Allows you to get the next RecordLocation after the <code>location</code> that 
	 * is in the journal.
	 * 
	 * @param location the reference location the is used to find the next location.
	 * To get the oldest location available in the journal, <code>location</code> 
	 * should be set to null.
	 * 
	 * 
	 * @return the next record location
	 * 
	 * @throws InvalidRecordLocationException if <code>location</code> parameter is out of range.  
	 *         It cannot be a location that is before the current mark. 
	 * @throws IllegalStateException if the journal is closed.
	 */
	public abstract RecordLocation getNextRecordLocation(RecordLocation location)
		throws InvalidRecordLocationException, IOException, IllegalStateException;


	/**
	 * Registers a <code>JournalEventListener</code> that will receive notifications from the Journal.
	 * 
	 * @param listener object that will receive journal events.
	 * @throws IllegalStateException if the journal is closed.
	 */
	public abstract void setJournalEventListener(JournalEventListener listener) throws IllegalStateException;
	
}
