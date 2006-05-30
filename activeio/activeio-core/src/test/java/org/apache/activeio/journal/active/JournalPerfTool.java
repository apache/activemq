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
package org.apache.activeio.journal.active;

import java.io.File;
import java.io.IOException;

import org.apache.activeio.journal.Journal;
import org.apache.activeio.journal.JournalPerfToolSupport;
import org.apache.activeio.journal.active.JournalImpl;

/**
 * A Performance statistics gathering tool for the JournalImpl based Journal.
 * 
 * @version $Revision: 1.1 $
 */
public class JournalPerfTool extends JournalPerfToolSupport {
	
	private int logFileSize = 1024*1000*5;
    private int logFileCount = 4;
	
	public static void main(String[] args) throws Exception {
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
			tool.logFileCount = Integer.parseInt(args[7]);
		}
		if( args.length > 8 ) {
			tool.logFileSize = Integer.parseInt(args[8]);
		}
		tool.exec();
	}

	/**
	 * @throws IOException
	 * @see org.apache.activeio.journal.JournalPerfToolSupport#createJournal()
	 */
	public Journal createJournal() throws IOException {
		return new JournalImpl( this.journalDirectory, logFileCount, logFileSize);
	}
	
}
