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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class DataFileAccessorTest {

    @Rule
    public TemporaryFolder dataDir = new TemporaryFolder();

    @Test
    public void testValidLocation() throws IOException {
        //Create file of size 1024
        final DataFileAccessor accessor = new DataFileAccessor(mock(Journal.class),
            newTestDataFile(1024));

        //The read check will add the offset and location size and will be 612
        //so should be fine as it's less than the set file size of 1024
        final Location location = new Location(0, 100);
        location.setSize(512);

        accessor.validateFileLength(location);
    }

    @Test(expected = IOException.class)
    public void testInvalidLocationSize() throws IOException {
        //Create file of size 1024
        final DataFileAccessor accessor = new DataFileAccessor(mock(Journal.class),
            newTestDataFile(1024));

        //Set a size that is too large so this should fail
        final Location location = new Location(0, 100);
        location.setSize(2048);

        accessor.validateFileLength(location);
    }

    @Test
    public void testValidateUsingRealFileLength() throws IOException {
        //Create file of size 1024
        final DataFile dataFile = newTestDataFile(1024);

        final DataFileAccessor accessor = new DataFileAccessor(mock(Journal.class), dataFile);

        //Set a bad length value on the dataFile so that the initial check fails
        //because the location is greater than dataFile.length
        //We should read the real file size (1024) which is greater than the
        //location size + offset so this should work
        dataFile.setLength(512);
        final Location location = new Location(0, 100);
        location.setSize(512);

        accessor.validateFileLength(location);
    }

    private DataFile newTestDataFile(int size) throws IOException {
        final DataFile dataFile = new DataFile(writeTestFile(1024), 0);
        assertEquals(1024, dataFile.length);
        return dataFile;
    }

    private File writeTestFile(int size) throws IOException {
        final File file = dataDir.newFile();
        final byte[] data = new byte[size];
        Arrays.fill(data, (byte)0);
        Files.write(Path.of(file.toURI()), data);
        assertEquals(1024, file.length());
        return file;
    }
}
