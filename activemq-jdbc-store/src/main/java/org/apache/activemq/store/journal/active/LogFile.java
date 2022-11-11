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
package org.apache.activemq.store.journal.active;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Allows read/append access to a LogFile.
 * 
 * @version $Revision: 1.1 $
 */
final public class LogFile {

    private final RandomAccessFile file;
    private final FileChannel channel;

    /** Prefered size. The size that the log file is set to when initilaized. */
    private final int initialSize;

    /** Where the we are in the file right now */
    private int currentOffset;
	private boolean disposed;
    
    public LogFile(File file, int initialSize) throws IOException {
        this.initialSize = initialSize;
        boolean initializationNeeeded = !file.exists();
        this.file = new RandomAccessFile(file, "rw");
        channel = this.file.getChannel();
        if( initializationNeeeded )
            resize();
        channel.position(0);
        reloadCurrentOffset();
    }

    /**
     * To avoid doing un-needed seeks.
     */
    private void seek(int offset) throws IOException {
        if( offset == currentOffset ) {
            if( currentOffset != channel.position() )
                throw new RuntimeException(" "+currentOffset+", "+channel.position() );                
            return;
        }
        channel.position(offset);
        currentOffset = offset;
    }
    private void reloadCurrentOffset() throws IOException {
        currentOffset= (int) channel.position();
    }
    private void addToCurrentOffset(int rc) {
        currentOffset+=rc;
    }
    
    public boolean loadAndCheckRecord(int offset, Record record) throws IOException {
        
        try { 
            // Read the next header
            seek(offset);        
            record.readHeader(file);
                    
            if (Record.isChecksumingEnabled()) {
                record.checksum(file);
            }            
            // Load the footer.
            seek(offset+record.getPayloadLength()+Record.RECORD_HEADER_SIZE);
            record.readFooter(file);
            
            addToCurrentOffset(record.getRecordLength());
            return true;
                
        } catch (IOException e) {
            reloadCurrentOffset();
            return false;
        }
    }
    
    public void resize() throws IOException {
        file.setLength(initialSize);
    }

    public void force() throws IOException {
        channel.force(false);
    }

    public void dispose() {
    	if( disposed )
    		return;
    	disposed=true;
        try {
			this.file.close();
		} catch (IOException e) {
		}
    }

    public void write(int offset, ByteBuffer buffer) throws IOException {
        
        try {

            int size = buffer.remaining();
            seek(offset);
            while (buffer.hasRemaining()) {
                channel.write(buffer);                
            }
            addToCurrentOffset(size);
            
        } catch (IOException e) {
            reloadCurrentOffset();
        }
    }

    public void readRecordHeader(int offset, Record record) throws IOException {
        seek(offset);  
        try {
            record.readHeader(file);
        } catch ( IOException e ) {
            reloadCurrentOffset();
            throw e;
        }
        addToCurrentOffset(Record.RECORD_HEADER_SIZE);
    }

    public void read(int offset, byte[] answer) throws IOException {
        seek(offset);
        file.readFully(answer);
        addToCurrentOffset(answer.length);
    }

    public void copyTo(File location) throws IOException {
        FileOutputStream fos = new FileOutputStream(location);
        channel.transferTo(0, channel.size(), fos.getChannel());
        fos.getChannel().force(false);
        fos.close();
    }
}
