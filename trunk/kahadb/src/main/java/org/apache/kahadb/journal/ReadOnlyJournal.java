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
package org.apache.kahadb.journal;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An AsyncDataManager that works in read only mode against multiple data directories.
 * Useful for reading back archived data files.
 */
public class ReadOnlyJournal extends Journal {
    
    private final ArrayList<File> dirs;

    public ReadOnlyJournal(final ArrayList<File> dirs) {
        this.dirs = dirs;
    }

    public synchronized void start() throws IOException {
        if (started) {
            return;
        }

        started = true;
                
        ArrayList<File> files = new ArrayList<File>();
        for (File directory : dirs) {
            final File d = directory;
            File[] f = d.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String n) {
                    return dir.equals(d) && n.startsWith(filePrefix);
                }
            });
            for (int i = 0; i < f.length; i++) {
                files.add(f[i]);
            }
        }
       
        for (File file : files) {
            try {
                String n = file.getName();
                String numStr = n.substring(filePrefix.length(), n.length());
                int num = Integer.parseInt(numStr);
                DataFile dataFile = new ReadOnlyDataFile(file, num, preferedFileLength);
                fileMap.put(dataFile.getDataFileId(), dataFile);
                totalLength.addAndGet(dataFile.getLength());
            } catch (NumberFormatException e) {
                // Ignore file that do not match the pattern.
            }
        }

        // Sort the list so that we can link the DataFiles together in the
        // right order.
        List<DataFile> list = new ArrayList<DataFile>(fileMap.values());
        Collections.sort(list);
        for (DataFile df : list) {
            dataFiles.addLast(df);
            fileByFileMap.put(df.getFile(), df);
        }
        
//        // Need to check the current Write File to see if there was a partial
//        // write to it.
//        if (!dataFiles.isEmpty()) {
//
//            // See if the lastSyncedLocation is valid..
//            Location l = lastAppendLocation.get();
//            if (l != null && l.getDataFileId() != dataFiles.getTail().getDataFileId().intValue()) {
//                l = null;
//            }
//            
//            // If we know the last location that was ok.. then we can skip lots
//            // of checking
//            try {
//                l = recoveryCheck(dataFiles.getTail(), l);
//                lastAppendLocation.set(l);
//            } catch (IOException e) {
//                LOG.warn("recovery check failed", e);
//            }
//        }
    }
    
    public synchronized void close() throws IOException {
        if (!started) {
            return;
        }
        accessorPool.close();
        fileMap.clear();
        fileByFileMap.clear();
        started = false;
    }

    
    public Location getFirstLocation() throws IllegalStateException, IOException {
        if( dataFiles.isEmpty() ) {
            return null;
        }
        
        DataFile first = dataFiles.getHead();
        Location cur = new Location();
        cur.setDataFileId(first.getDataFileId());
        cur.setOffset(0);
        cur.setSize(0);
        return getNextLocation(cur);
    }
    
    @Override
    public synchronized boolean delete() throws IOException {
        throw new RuntimeException("Cannot delete a ReadOnlyAsyncDataManager");
    }    
}
