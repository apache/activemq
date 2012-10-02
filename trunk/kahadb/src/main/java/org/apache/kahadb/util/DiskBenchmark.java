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
package org.apache.kahadb.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * This class is used to get a benchmark the raw disk performance.
 */
public class DiskBenchmark {

    boolean verbose;
    // reads and writes work with 4k of data at a time.
    int bs=1024*4; 
    // Work with 100 meg file.
    long size=1024*1024*500; 
    long sampleInterval = 10*1000; 
    
    public static void main(String[] args) {

        DiskBenchmark benchmark = new DiskBenchmark();
        args = CommandLineSupport.setOptions(benchmark, args);
        ArrayList<String> files = new ArrayList<String>();
        if (args.length == 0) {
            files.add("disk-benchmark.dat");
        } else {
            files.addAll(Arrays.asList(args));
        }

        for (String f : files) {
            try {
                File file = new File(f);
                if (file.exists()) {
                    System.out.println("File " + file + " allready exists, will not benchmark.");
                } else {
                    System.out.println("Benchmarking: " + file.getCanonicalPath());
                    Report report = benchmark.benchmark(file);
                    file.delete();
                    System.out.println(report.toString());
                }
            } catch (Throwable e) {
                if (benchmark.verbose) {
                    System.out.println("ERROR:");
                    e.printStackTrace(System.out);
                } else {
                    System.out.println("ERROR: " + e);
                }
            }
        }

    }
    
    public static class Report {

        public int size;
        
        public int writes;
        public long writeDuration;
        
        public int syncWrites;
        public long syncWriteDuration;
        
        public int reads;
        public long readDuration;

        @Override
        public String toString() {
            return 
            "Writes: \n" +
            "  "+writes+" writes of size "+size+" written in "+(writeDuration/1000.0)+" seconds.\n"+
            "  "+getWriteRate()+" writes/second.\n"+
            "  "+getWriteSizeRate()+" megs/second.\n"+
            "\n"+
            "Sync Writes: \n" +
            "  "+syncWrites+" writes of size "+size+" written in "+(syncWriteDuration/1000.0)+" seconds.\n"+
            "  "+getSyncWriteRate()+" writes/second.\n"+
            "  "+getSyncWriteSizeRate()+" megs/second.\n"+
            "\n"+
            "Reads: \n" +
            "  "+reads+" reads of size "+size+" read in "+(readDuration/1000.0)+" seconds.\n"+
            "  "+getReadRate()+" writes/second.\n"+
            "  "+getReadSizeRate()+" megs/second.\n"+
            "\n"+
            "";
        }

        private float getWriteSizeRate() {
            float rc = writes;
            rc *= size;
            rc /= (1024*1024); // put it in megs
            rc /= (writeDuration/1000.0); // get rate. 
            return rc;
        }

        private float getWriteRate() {
            float rc = writes;
            rc /= (writeDuration/1000.0); // get rate. 
            return rc;
        }
        
        private float getSyncWriteSizeRate() {
            float rc = syncWrites;
            rc *= size;
            rc /= (1024*1024); // put it in megs
            rc /= (syncWriteDuration/1000.0); // get rate. 
            return rc;
        }

        private float getSyncWriteRate() {
            float rc = syncWrites;
            rc /= (syncWriteDuration/1000.0); // get rate. 
            return rc;
        }
        private float getReadSizeRate() {
            float rc = reads;
            rc *= size;
            rc /= (1024*1024); // put it in megs
            rc /= (readDuration/1000.0); // get rate. 
            return rc;
        }

        private float getReadRate() {
            float rc = reads;
            rc /= (readDuration/1000.0); // get rate. 
            return rc;
        }

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            this.size = size;
        }

        public int getWrites() {
            return writes;
        }

        public void setWrites(int writes) {
            this.writes = writes;
        }

        public long getWriteDuration() {
            return writeDuration;
        }

        public void setWriteDuration(long writeDuration) {
            this.writeDuration = writeDuration;
        }

        public int getSyncWrites() {
            return syncWrites;
        }

        public void setSyncWrites(int syncWrites) {
            this.syncWrites = syncWrites;
        }

        public long getSyncWriteDuration() {
            return syncWriteDuration;
        }

        public void setSyncWriteDuration(long syncWriteDuration) {
            this.syncWriteDuration = syncWriteDuration;
        }

        public int getReads() {
            return reads;
        }

        public void setReads(int reads) {
            this.reads = reads;
        }

        public long getReadDuration() {
            return readDuration;
        }

        public void setReadDuration(long readDuration) {
            this.readDuration = readDuration;
        }
    }


    public Report benchmark(File file) throws IOException {
        Report rc = new Report();
        
        // Initialize the block we will be writing to disk.
        byte []data = new byte[bs];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte)('a'+(i%26));
        }
        
        rc.size = data.length;
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(size);
        
        // Figure out how many writes we can do in the sample interval.
        long start = System.currentTimeMillis();
        long now = System.currentTimeMillis();
        int ioCount=0;
        while( true ) {
            if( (now-start)>sampleInterval ) {
                break;
            }
            raf.seek(0);
            for( long i=0; i+data.length < size; i+=data.length) {
                raf.write(data);
                ioCount++;
                now = System.currentTimeMillis();
                if( (now-start)>sampleInterval ) {
                    break;
                }
            }
            // Sync to disk so that the we actually write the data to disk.. otherwise 
            // OS buffering might not really do the write.
            raf.getFD().sync();
        }
        raf.getFD().sync();
        raf.close();
        now = System.currentTimeMillis();
        
        rc.size = data.length;
        rc.writes = ioCount;
        rc.writeDuration = (now-start);

        raf = new RandomAccessFile(file, "rw");
        start = System.currentTimeMillis();
        now = System.currentTimeMillis();
        ioCount=0;
        while( true ) {
            if( (now-start)>sampleInterval ) {
                break;
            }
            for( long i=0; i+data.length < size; i+=data.length) {
                raf.seek(i);
                raf.write(data);
                raf.getFD().sync();
                ioCount++;
                now = System.currentTimeMillis();
                if( (now-start)>sampleInterval ) {
                    break;
                }
            }
        }
        raf.close();
        now = System.currentTimeMillis();
        rc.syncWrites = ioCount;
        rc.syncWriteDuration = (now-start);

        raf = new RandomAccessFile(file, "rw");
        start = System.currentTimeMillis();
        now = System.currentTimeMillis();
        ioCount=0;
        while( true ) {
            if( (now-start)>sampleInterval ) {
                break;
            }
            raf.seek(0);
            for( long i=0; i+data.length < size; i+=data.length) {
                raf.seek(i);
                raf.readFully(data);
                ioCount++;
                now = System.currentTimeMillis();
                if( (now-start)>sampleInterval ) {
                    break;
                }
            }
        }
        raf.close();
        
        rc.reads = ioCount;
        rc.readDuration = (now-start);
        return rc;
    }


    public boolean isVerbose() {
        return verbose;
    }


    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }


    public int getBs() {
        return bs;
    }


    public void setBs(int bs) {
        this.bs = bs;
    }


    public long getSize() {
        return size;
    }


    public void setSize(long size) {
        this.size = size;
    }


    public long getSampleInterval() {
        return sampleInterval;
    }


    public void setSampleInterval(long sampleInterval) {
        this.sampleInterval = sampleInterval;
    }

}
