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

package org.apache.activemq.web.view;

import java.util.Enumeration;
import java.util.NoSuchElementException;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Subset of an Enumeration which retrieves a "page" of the Enumerations results, skipping elements up to the start of
 * the page, and not returning any after the end of the page.
 */
public class TestEnumerationPage {
    protected Enumeration   testSource;
    protected Object[]      eles;

    @Before
    public void setTest () {
        this.eles = new String[5];
        this.eles[0] = "zero";
        this.eles[1] = "one";
        this.eles[2] = "two";
        this.eles[3] = "three";
        this.eles[4] = "four";

        this.testSource = new Enumeration () {
            int cur = 0;

            public boolean hasMoreElements () {
                if ( cur < eles.length ) {
                    return  true;
                }

                return  false;
            }

            public Object   nextElement () {
		Object	result;

                if ( cur >= eles.length ) {
                    throw   new NoSuchElementException("TEST ERROR");
                }

                result = eles[cur];
                cur++;

                return  result;
            }
        } ;
    }

    @Test
    public void testEnumerationPage1 () {
        EnumerationPage tgt;

        tgt = new EnumerationPage(this.testSource, 0, 5);
        verifyResults(tgt, 0, 5);
    }

    @Test
    public void testEnumerationPage2 () {
        EnumerationPage tgt;

        tgt = new EnumerationPage(this.testSource, 0, 1);
        verifyResults(tgt, 0, 1);
    }

    @Test
    public void testEnumerationPage3 () {
        EnumerationPage tgt;

        tgt = new EnumerationPage(this.testSource, 1, 4);
        verifyResults(tgt, 1, 4);
    }

    @Test
    public void testEnumerationPage4 () {
        EnumerationPage tgt;

        tgt = new EnumerationPage(this.testSource, 2, 3);
        verifyResults(tgt, 2, 3);
    }

    @Test
    public void testEnumerationPage5 () {
        EnumerationPage tgt;

        tgt = new EnumerationPage(this.testSource, 2, 2);
        verifyResults(tgt, 2, 2);
    }

    /**
     * Verify the EnumerationPage given returns the elements specified, from start to end (exlcusive), and correctly
     * fails to return any more elements.
     */
    protected void  verifyResults (EnumerationPage enumPage, long start, long end) {
        long    cur;
        boolean haveExc;

        cur = start;
        while ( cur < end ) {
            assertTrue(enumPage.hasMoreElements());
            assertEquals(this.eles[(int) cur], enumPage.nextElement());
            cur++;
        }

        assertFalse(enumPage.hasMoreElements());
        haveExc = false;

        try {
            enumPage.nextElement();
        }
        catch ( NoSuchElementException nse_exc ) {
            haveExc = true;
        }

        assertTrue(haveExc);
    }
}
