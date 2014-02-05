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

import java.util.Enumeration;
import java.util.NoSuchElementException;

/**
 * Subset of an Enumeration which retrieves a "page" of the Enumerations results, skipping elements up to the start of
 * the page, and not returning any after the end of the page.
 */
public class EnumerationPage implements Enumeration {
    protected Enumeration   sourceEnumeration;
    protected long          startOffset;
    protected long          endOffset;
    protected long          currentOffset;

    /**
     * Create the EnumerationPage with the given source Enumeration, starting offset, and ending offset which determine
     * just which elements will be returned by the EnumerationPage.  Note the ending offset is excluded.  Also note
     * the start of the source is the current "next" element; if elements have already been taken from the source (via
     * nextElement()), they are ignored.
     *
     * @param   source - the Enumeration that is the source of data returned.
     * @param   start - offset from the start of the given enumeration at which elements will first be returned (e.g.
     *          0 means the first element in the source will be the first returned).
     * @param   end - offset from the start of the given enumeration at which elements will stop being returned,
     *          exclusive (e.g. 1 means the last element returned is the first in the source).
     */
    public EnumerationPage (Enumeration source, long start, long end) {
        this.sourceEnumeration = source;
        this.startOffset = start;
        this.endOffset = end;
        this.currentOffset = 0;
    }

    public boolean  hasMoreElements () {
        this.seekToElement(this.startOffset);

        if ( this.currentOffset >= this.endOffset ) {
            return  false;
        }

        return  this.sourceEnumeration.hasMoreElements();
    }

    public Object   nextElement () {
        Object  result;

        if ( this.currentOffset >= this.endOffset ) {
            throw   new NoSuchElementException("exceeded the end of the enumeration page at offset " + this.currentOffset);
        }

        this.seekToElement(this.startOffset);
        result = this.sourceEnumeration.nextElement();
        this.currentOffset++;

        return  result;
    }

    protected void  seekToElement (long offset) {
        while ( ( this.currentOffset < offset ) && ( this.currentOffset < this.endOffset) &&
                ( this.sourceEnumeration.hasMoreElements() ) ) {
            this.sourceEnumeration.nextElement();
            this.currentOffset++;
        }
    }
}
