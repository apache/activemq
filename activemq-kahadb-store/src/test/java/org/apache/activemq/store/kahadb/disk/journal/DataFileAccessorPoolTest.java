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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DataFileAccessorPoolTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void disposeUnused() throws Exception {

        Journal journal = mock(Journal.class);

        DataFileAccessorPool underTest = new DataFileAccessorPool(journal);

        DataFile dataFile = new DataFile(new File(temporaryFolder.getRoot(), "aa"), 1);
        underTest.closeDataFileAccessor(underTest.openDataFileAccessor(dataFile));

        assertEquals("one in the pool", 1, underTest.size());
        underTest.disposeUnused();

        assertEquals("0 in the pool", 0, underTest.size());

        verify(journal, times(1)).getInflightWrites();
    }

}