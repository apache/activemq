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
package org.apache.activemq.util;

import java.io.File;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LockFileTest {

    @Test
    public void testNoDeleteOnUnlockIfNotLocked() throws Exception {

        File lockFile = new File(IOHelper.getDefaultDataDirectory(), "lockToTest1");
        IOHelper.mkdirs(lockFile.getParentFile());
        lockFile.createNewFile();

        LockFile underTest = new LockFile(lockFile, true);

        underTest.lock();

        // will fail on windows b/c the file is open
        if ( lockFile.delete() ) {

            assertFalse("no longer valid", underTest.keepAlive());

            // a slave gets in
            lockFile.createNewFile();

            underTest.unlock();

            assertTrue("file still exists after unlock when not locked", lockFile.exists());
        }

    }

    @Test
    public void testDeleteOnUnlockIfLocked() throws Exception {

        File lockFile = new File(IOHelper.getDefaultDataDirectory(), "lockToTest2");
        IOHelper.mkdirs(lockFile.getParentFile());
        lockFile.createNewFile();

        LockFile underTest = new LockFile(lockFile, true);

        underTest.lock();

        assertTrue("valid", underTest.keepAlive());

        underTest.unlock();

        assertFalse("file deleted on unlock", lockFile.exists());

    }
}
