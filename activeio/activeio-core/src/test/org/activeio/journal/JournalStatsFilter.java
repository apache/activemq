/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.activeio.journal;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.activeio.packet.Packet;
import org.activeio.stats.CountStatisticImpl;
import org.activeio.stats.IndentPrinter;
import org.activeio.stats.TimeStatisticImpl;

/**
 * A Journal filter that captures performance statistics of the filtered Journal.
 * 
 * @version $Revision: 1.1 $
 */
public class JournalStatsFilter implements Journal {
	
	private final TimeStatisticImpl writeLatency = new TimeStatisticImpl("writeLatency", "The amount of time that is spent waiting for a record to be written to the Journal"); 
	private final CountStatisticImpl writeRecordsCounter = new CountStatisticImpl("writeRecordsCounter","The number of records that have been written by the Journal");
	private final CountStatisticImpl writeBytesCounter = new CountStatisticImpl("writeBytesCounter","The number of bytes that have been written by the Journal");
	private final TimeStatisticImpl synchedWriteLatency = new TimeStatisticImpl(writeLatency, "synchedWriteLatency", "The amount of time that is spent waiting for a synch record to be written to the Journal"); 
	private final TimeStatisticImpl unsynchedWriteLatency = new TimeStatisticImpl(writeLatency, "unsynchedWriteLatency", "The amount of time that is spent waiting for a non synch record to be written to the Journal"); 	
	private final TimeStatisticImpl readLatency = new TimeStatisticImpl("readLatency", "The amount of time that is spent waiting for a record to be read from the Journal"); 
	private final CountStatisticImpl readBytesCounter = new CountStatisticImpl("readBytesCounter","The number of bytes that have been read by the Journal");
	
	private final Journal next;
	private boolean detailedStats;

	
	/**
	 * Creates a JournalStatsFilter that captures performance information of <code>next</next>. 
	 * @param next
	 */
	public JournalStatsFilter(Journal next) {
		this.next = next;
	}
	
	/**
	 * @see org.codehaus.activemq.journal.Journal#write(byte[], boolean)
	 */
	public RecordLocation write(Packet data, boolean sync) throws IOException {
		//writeWaitTimeStat
		long start = System.currentTimeMillis();
		RecordLocation answer = next.write(data, sync);
		long end = System.currentTimeMillis();
		
		writeRecordsCounter.increment();
		writeBytesCounter.add(data.remaining());
		if( sync )
			synchedWriteLatency.addTime(end-start);
		else 
			unsynchedWriteLatency.addTime(end-start);
		return answer;
	}

	/**
	 * @see org.codehaus.activemq.journal.Journal#read(org.codehaus.activemq.journal.RecordLocation)
	 */
	public Packet read(RecordLocation location)
			throws InvalidRecordLocationException, IOException {
		
		long start = System.currentTimeMillis();
		Packet answer = next.read(location);		
		long end = System.currentTimeMillis();
		
		readBytesCounter.add(answer.remaining());
		readLatency.addTime(end-start);
		return answer;
	}

	/**
	 * @see org.codehaus.activemq.journal.Journal#setMark(org.codehaus.activemq.journal.RecordLocation, boolean)
	 */
	public void setMark(RecordLocation recordLocator, boolean force)
			throws InvalidRecordLocationException, IOException {
		next.setMark(recordLocator, force);
	}

	/**
	 * @see org.codehaus.activemq.journal.Journal#getMark()
	 */
	public RecordLocation getMark() {
		return next.getMark();
	}

	/**
	 * @see org.codehaus.activemq.journal.Journal#close()
	 */
	public void close() throws IOException {
		next.close();
	}
	
	/**
	 * @see org.codehaus.activemq.journal.Journal#setJournalEventListener(org.codehaus.activemq.journal.JournalEventListener)
	 */
	public void setJournalEventListener(JournalEventListener eventListener) {
	    next.setJournalEventListener(eventListener);
	}

	/**
	 * @see org.codehaus.activemq.journal.Journal#getNextRecordLocation(org.codehaus.activemq.journal.RecordLocation)
	 */
	public RecordLocation getNextRecordLocation(RecordLocation lastLocation)
			throws IOException, InvalidRecordLocationException {		
		return next.getNextRecordLocation(lastLocation);
	}
	
	/**
	 * Writes the gathered statistics to the <code>out</code> object.
	 * 
	 * @param out
	 */
    public void dump(IndentPrinter out) {
        out.printIndent();
        out.println("Journal Stats {");        
        out.incrementIndent();
        out.printIndent();
        out.println("Throughput           : "+ getThroughputKps() +" k/s and " + getThroughputRps() +" records/s" );
        out.printIndent();
        out.println("Latency with force   : "+ getAvgSyncedLatencyMs() +" ms"  );
        out.printIndent();
        out.println("Latency without force: "+ getAvgUnSyncedLatencyMs() +" ms"  );

        out.printIndent();
        out.println("Raw Stats {");
        out.incrementIndent();
                
        out.printIndent();
        out.println(writeRecordsCounter);
        out.printIndent();
        out.println(writeBytesCounter);
        out.printIndent();
        out.println(writeLatency);
        out.incrementIndent();
        out.printIndent();
        out.println(synchedWriteLatency);
        out.printIndent();
        out.println(unsynchedWriteLatency);
        out.decrementIndent();

        out.printIndent();
        out.println(readBytesCounter);
        
        out.printIndent();
        out.println(readLatency);        
        out.decrementIndent();
        out.printIndent();
        out.println("}");
        
        out.decrementIndent();
        out.printIndent();
        out.println("}");

    }

    /**
     * Dumps the stats to a String.
     * 
     * @see java.lang.Object#toString()
     */
	public String toString() {
		if( detailedStats ) {
			StringWriter w = new StringWriter();
			PrintWriter pw = new PrintWriter(w);		
			dump(new IndentPrinter(pw, "  "));
			return w.getBuffer().toString();
		} else {
			StringWriter w = new StringWriter();
			PrintWriter pw = new PrintWriter(w);
			IndentPrinter out = new IndentPrinter(pw, "  ");
	        out.println("Throughput           : "+ getThroughputKps() +" k/s and " + getThroughputRps() +" records/s");
	        out.printIndent();
	        out.println("Latency with force   : "+getAvgSyncedLatencyMs()+" ms"  );
	        out.printIndent();
	        out.println("Latency without force: "+getAvgUnSyncedLatencyMs()+" ms"  );
			return w.getBuffer().toString();			
		}
    }

	/**
	 * @param detailedStats true if details stats should be displayed by <code>toString()</code> and <code>dump</code>
	 * @return
	 */
	public JournalStatsFilter enableDetailedStats(boolean detailedStats) {		
		this.detailedStats = detailedStats;
		return this;
	}

	/**
	 * Gets the average throughput in k/s.
	 * 
	 * @return the average throughput in k/s.
	 */
	public double getThroughputKps() {
		 long totalTime = writeBytesCounter.getLastSampleTime()-writeBytesCounter.getStartTime(); 
		 return (((double)writeBytesCounter.getCount()/(double)totalTime)/(double)1024)*1000;
	}

	/**
	 * Gets the average throughput in records/s.
	 * 
	 * @return the average throughput in records/s.
	 */
	public double getThroughputRps() {
		 long totalTime = writeRecordsCounter.getLastSampleTime()-writeRecordsCounter.getStartTime(); 
		 return (((double)writeRecordsCounter.getCount()/(double)totalTime))*1000;
	}

	/**
	 * Gets the average number of writes done per second
	 * 
	 * @return the average number of writes in w/s.
	 */
	public double getWritesPerSecond() {
		 return writeLatency.getAveragePerSecond();
	}

	/**
	 * Gets the average sync write latency in ms.
	 * 
	 * @return the average sync write latency in ms.
	 */
	public double getAvgSyncedLatencyMs() {
		return synchedWriteLatency.getAverageTime();
	}

	/**
	 * Gets the average non sync write latency in ms.
	 * 
	 * @return the average non sync write latency in ms.
	 */
	public double getAvgUnSyncedLatencyMs() {
		return unsynchedWriteLatency.getAverageTime();
	}
	
	/**
	 * Resets the stats sample.
	 */
	public void reset() {
		writeLatency.reset(); 
		writeBytesCounter.reset();
		writeRecordsCounter.reset();
		synchedWriteLatency.reset(); 
		unsynchedWriteLatency.reset(); 	
		readLatency.reset(); 
		readBytesCounter.reset();
	}
}