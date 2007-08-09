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
package org.apache.activemq.kaha.impl.async;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import junit.framework.TestCase;
import org.apache.activemq.kaha.impl.async.JournalFacade.RecordLocationFacade;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Tests the Location Class
 * 
 * @version $Revision: 1.1 $
 */
public class LocationTest extends TestCase {
    private static final transient Log LOG = LogFactory.getLog(LocationTest.class);

    @SuppressWarnings("unchecked")
    public synchronized void testRecordLocationImplComparison() throws IOException {
        Location l1 = new Location();
        l1.setDataFileId(0);
        l1.setOffset(5);
        Location l2 = new Location(l1);
        l2.setOffset(10);
        Location l3 = new Location(l2);
        l3.setDataFileId(2);
        l3.setOffset(0);

        assertTrue(l1.compareTo(l2) < 0);

        // Sort them using a list. Put them in the wrong order.
        ArrayList<RecordLocationFacade> l = new ArrayList<RecordLocationFacade>();
        l.add(new RecordLocationFacade(l2));
        l.add(new RecordLocationFacade(l3));
        l.add(new RecordLocationFacade(l1));
        Collections.sort(l);

        // Did they get sorted to the correct order?
        LOG.debug(l.get(0));
        assertSame(l.get(0).getLocation(), l1);
        assertSame(l.get(1).getLocation(), l2);
        assertSame(l.get(2).getLocation(), l3);
    }
}
