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
package org.apache.kahadb.replication;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.apache.kahadb.journal.Location;
import org.apache.kahadb.replication.pb.PBFileInfo;
import org.apache.kahadb.replication.pb.PBJournalLocation;

public class ReplicationSupport {
    
    static public PBJournalLocation convert(Location loc) {
        if( loc==null ) {
            return null;
        }
        return new PBJournalLocation().setFileId(loc.getDataFileId()).setOffset(loc.getOffset());
    }
    
    static public Location convert(PBJournalLocation location) {
        Location rc = new Location();
        rc.setDataFileId(location.getFileId());
        rc.setOffset(location.getOffset());
        return rc;
    }


    static public long copyAndChecksum(File input, File output) throws IOException {
        FileInputStream is = null;
        FileOutputStream os = null;
        try {
            is = new FileInputStream(input);
            os = new FileOutputStream(output);

            byte buffer[] = new byte[1024 * 4];
            int c;

            Checksum checksum = new Adler32();
            while ((c = is.read(buffer)) >= 0) {
                os.write(buffer, 0, c);
                checksum.update(buffer, 0, c);
            }
            return checksum.getValue();

        } finally {
            try {
                is.close();
            } catch(Throwable e) {
            }
            try {
                os.close();
            } catch(Throwable e) {
            }
        }
    }

    public static PBFileInfo createInfo(String name, File file, long start, long length) throws IOException {
        PBFileInfo rc = new PBFileInfo();
        rc.setName(name);
        rc.setChecksum(checksum(file, start, length));
        rc.setStart(start);
        rc.setEnd(length);
        return rc;
    }

    public static long checksum(File file, long start, long end) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        try {
            Checksum checksum = new Adler32();
            byte buffer[] = new byte[1024 * 4];
            int c;
            long pos = start;
            raf.seek(start);

            while (pos < end && (c = raf.read(buffer, 0, (int) Math.min(end - pos, buffer.length))) >= 0) {
                checksum.update(buffer, 0, c);
                pos += c;
            }

            return checksum.getValue();
        } finally {
            try {
                raf.close();
            } catch (Throwable e) {
            }
        }
    }

}
