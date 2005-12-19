/** 
 * 
 * Copyright 2004 Hiram Chirino
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
 * 
 **/
package org.activeio.journal;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

import org.activeio.packet.ByteArrayPacket;

/**
 * Provides the base class uses to run performance tests against a Journal.
 * Should be subclassed to customize for specific journal implementation.
 * 
 * @version $Revision: 1.1 $
 */
abstract public class JournalPerfToolSupport implements JournalEventListener {

	private JournalStatsFilter journal;
	private Random random = new Random();
	private byte data[];
	private int workerCount=0;	
	private PrintWriter statWriter;
	// Performance test Options
	
	// The output goes here:
	protected File journalDirectory = new File("journal-logs");
	protected File statCSVFile = new File("stats.csv");;

	// Controls how often we start a new batch of workers.
	protected int workerIncrement=20;
	protected long incrementDelay=1000*20;
	protected boolean verbose=true;

	// Worker configuration.
	protected int recordSize=1024;
	protected int syncFrequency=15;	
	protected int workerThinkTime=100;

    private final class Worker implements Runnable {
		public void run() {
			int i=random.nextInt()%syncFrequency;
			while(true) {
				boolean sync=false;
				
				if( syncFrequency>=0 && (i%syncFrequency)==0 ) {
					sync=true;
				}				
				try {
					journal.write(new ByteArrayPacket(data), sync);
					Thread.sleep(workerThinkTime);
				} catch (Exception e) {
					e.printStackTrace();
					return;
				}
				i++;						
			}					
		}
	}	
	
    /**
     * @throws IOException
	 * 
	 */
	protected void exec() throws Exception {
		
		System.out.println("Client threads write records using: Record Size: "+recordSize+", Sync Frequency: "+syncFrequency+", Worker Think Time: "+workerThinkTime);

		// Create the record and fill it with some values.
		data = new byte[recordSize];
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte)i;
		}
		
		if( statCSVFile!=null ) {
			statWriter = new PrintWriter(new FileOutputStream(statCSVFile));
			statWriter.println("Threads,Throughput (k/s),Forcd write latency (ms),Throughput (records/s)");
		}
		
        if( journalDirectory.exists() ) {
        	deleteDir(journalDirectory);
        }		
        journal = new JournalStatsFilter(createJournal()).enableDetailedStats(verbose);
        journal.setJournalEventListener(this);
		
        try {        	
        	
        	// Wait a little to see the worker affect the stats.
        	// Increment the number of workers every few seconds.
        	while(true) {
        		System.out.println("Starting "+workerIncrement+" Workers...");
            	for(int i=0;i <workerIncrement;i++) {
                	new Thread(new Worker()).start();
                	workerCount++;
            	}
            				
            	// Wait a little to see the worker affect the stats.
            	System.out.println("Waiting "+(incrementDelay/1000)+" seconds before next Stat sample.");
            	Thread.sleep(incrementDelay);
            	displayStats();
            	journal.reset();
        	}
        	
        	
        } finally {
        	journal.close();
        }
	}

	private void displayStats() {		
		System.out.println("Stats at "+workerCount+" workers.");
		System.out.println(journal);        	
		if( statWriter!= null ) {
			statWriter.println(""+workerCount+","+journal.getThroughputKps()+","+journal.getAvgSyncedLatencyMs()+","+journal.getThroughputRps());
			statWriter.flush();
		}
	}

	/**
	 * @return
	 */
	abstract public Journal createJournal() throws Exception;

	static private void deleteDir(File f) {
		File[] files = f.listFiles();
		for (int i = 0; i < files.length; i++) {
			File file = files[i];
			file.delete();
		}
		f.delete();
	}
    
    
	public void overflowNotification(RecordLocation safeLocation) {
		try {
			System.out.println("Mark set: "+safeLocation);
			journal.setMark(safeLocation, false);
		} catch (InvalidRecordLocationException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
}
