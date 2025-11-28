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
package org.apache.activemq.store.kahadb.disk.journal;

import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;

import org.apache.activemq.util.IOHelper;
import org.junit.Test;

/**
 * Verify corrupted sizes are rejected without allocating huge buffers.
 */
public class DataFileAccessorCorruptionTest {

    @Test
    public void testRejectsOversizedRecord() throws Exception {
        // minimal journal setup with a single empty data file
        final File dir = new File(IOHelper.getDefaultDataDirectory(), "DataFileAccessorCorruptionTest");
        dir.mkdirs();
        final Journal journal = new Journal();
        journal.setDirectory(dir);
        journal.setMaxFileLength(1024 * 1024); // 1MB max file length
        journal.start();

        try {
            // fabricate a location claiming a size larger than maxFileLength
            Location bogus = new Location();
            bogus.setOffset(0);
            bogus.setSize(journal.getMaxFileLength() + 1024);
            bogus.setDataFileId(0);

            DataFile dataFile = new DataFile(new File(dir, "db-0.log"), 0);
            DataFileAccessor accessor = new DataFileAccessor(journal, dataFile);
            assertThrows(IOException.class, () -> accessor.readRecord(bogus));
        } finally {
            journal.close();
        }
    }
}
