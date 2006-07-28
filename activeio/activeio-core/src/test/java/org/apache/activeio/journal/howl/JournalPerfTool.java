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
package org.apache.activeio.journal.howl;

import java.io.File;

import org.apache.activeio.journal.Journal;
import org.apache.activeio.journal.JournalPerfToolSupport;
import org.apache.activeio.journal.howl.HowlJournal;
import org.objectweb.howl.log.Configuration;

/**
 * A Performance statistics gathering tool for the HOWL based Journal.
 * 
 * @version $Revision: 1.1 $
 */
public class JournalPerfTool extends JournalPerfToolSupport {
	
    private int maxLogFiles=  2;
	private int bufferSize = 1024*4;
	private int maxBuffers = 20;
	private int maxBlocksPerFile = 100;
	
	public static void main(String[] args) throws Exception {
		
		try {
			JournalPerfTool tool = new JournalPerfTool();
			if( args.length > 0 ) {
				tool.journalDirectory = new File(args[0]);
			}
			if( args.length > 1 ) {
				tool.workerIncrement = Integer.parseInt(args[1]);
			}
			if( args.length > 2 ) {
				tool.incrementDelay = Long.parseLong(args[2]);
			}
			if( args.length > 3 ) {
				tool.verbose = Boolean.getBoolean(args[3]);
			}
			if( args.length > 4 ) {
				tool.recordSize = Integer.parseInt(args[4]);
			}
			if( args.length > 5 ) {
				tool.syncFrequency = Integer.parseInt(args[5]);
			}
			if( args.length > 6 ) {
				tool.workerThinkTime = Integer.parseInt(args[6]);
			}
			
			if( args.length > 7 ) {
				tool.maxLogFiles = Integer.parseInt(args[7]);
			}
			if( args.length > 8 ) {
				tool.bufferSize = Integer.parseInt(args[8]);
			}
			if( args.length > 9 ) {
				tool.maxBuffers = Integer.parseInt(args[9]);
			}
			if( args.length > 10 ) {
				tool.maxBlocksPerFile = Integer.parseInt(args[10]);
			}
			tool.exec();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	public Journal createJournal() throws Exception {
		Configuration c = new Configuration();
		c.setLogFileDir(journalDirectory.getPath());
		c.setMaxLogFiles(maxLogFiles);
		c.setBufferSize(bufferSize);
		c.setMaxBuffers(maxBuffers);
		c.setMaxBlocksPerFile(maxBlocksPerFile);
		return new HowlJournal( c );
	}
	
}
