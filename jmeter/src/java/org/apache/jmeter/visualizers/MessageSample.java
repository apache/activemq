/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jmeter.visualizers;


import java.io.Serializable;

public class MessageSample implements Serializable, Comparable {

    public long count;
    public long processed;
    public long data;

    public MessageSample(long num, long data, long processed) {
        this.count = num;
        this.data = data;
        this.processed = processed;
    }

    /**
     * @return Returns the count.
     */
    public long getCount() {
        return count;
    }

    /**
     * @param count The count to set.
     */
    public void setCount(long count) {
        this.count = count;
    }

    /**
     * @return Returns the data.
     */
    public long getData() {
        return data;
    }

    /**
     * @param data The data to set.
     */
    public void setData(long data) {
        this.data = data;
    }


    /**
     * @return Returns the processed.
     */
    public long getProcessed() {
        return processed;
    }

    /**
     * @param processed The processed to set.
     */
    public void setProcessed(long processed) {
        this.processed = processed;
    }

    public MessageSample() {
    }

    /* (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(Object o) {
        MessageSample oo = (MessageSample) o;
        return ((count - oo.count) < 0 ? -1 : (count == oo.count ? 0 : 1));
    }

}
