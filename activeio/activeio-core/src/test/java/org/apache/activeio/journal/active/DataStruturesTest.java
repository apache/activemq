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
package org.apache.activeio.journal.active;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.activeio.journal.active.Location;

import junit.framework.TestCase;

/**
 * Tests the data structures used JournalImpl
 * 
 * @version $Revision: 1.1 $
 */
public class DataStruturesTest extends TestCase {
        
    synchronized public void testRecordLocationImplComparison() throws IOException {
        Location l1 = new Location(0, 1); 
        Location l2 = new Location(0, 2);
        Location l3 = new Location(0, 3);

        assertTrue( l1.compareTo(l2)<0 );
        
        // Sort them using a list.  Put them in the wrong order.
        ArrayList l = new ArrayList();
        l.add(l2);
        l.add(l3);
        l.add(l1);        
        Collections.sort(l);
        
        // Did they get sorted to the correct order?
        assertSame( l.get(0), l1 );
        assertSame( l.get(1), l2 );
        assertSame( l.get(2), l3 );
    }
}
