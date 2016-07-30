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

import java.io.File;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DataFileAccessorPoolTest {
    private Mockery context;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        context = new Mockery() {
            {
                setImposteriser(ClassImposteriser.INSTANCE);
            }
        };
    }

    @Test
    public void disposeUnused() throws Exception {

        final Journal journal = context.mock(Journal.class);

        DataFileAccessorPool underTest = new DataFileAccessorPool(journal);

        context.checking(new Expectations(){{exactly(1).of(journal).getInflightWrites();}});

        DataFile dataFile = new DataFile(new File(temporaryFolder.getRoot(), "aa"), 1);
        underTest.closeDataFileAccessor(underTest.openDataFileAccessor(dataFile));

        assertEquals("one in the pool", 1, underTest.size());
        underTest.disposeUnused();

        assertEquals("0 in the pool", 0, underTest.size());

        context.assertIsSatisfied();
    }

}