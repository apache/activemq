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
package org.apache.activemq.perf;

/**
 * @version $Revision: 1.3 $
 */
public class PerfRate {

    protected int totalCount;
    protected int count;
    protected long startTime = System.currentTimeMillis();

    /**
     * @return Returns the count.
     */
    public int getCount() {
        return totalCount;
    }

    synchronized public void increment() {
        totalCount++;
        count++;
    }

    public int getRate() {
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        int result = (int)((count * 1000) / totalTime);
        return result;
    }

    /**
     * Resets the rate sampling.
     */
    synchronized public PerfRate cloneAndReset() {
        PerfRate rc = new PerfRate();
        rc.totalCount = totalCount;
        rc.count = count;
        rc.startTime = startTime;
        count = 0;
        startTime = System.currentTimeMillis();
        return rc;
    }

    /**
     * Resets the rate sampling.
     */
    public void reset() {
        count = 0;
        startTime = System.currentTimeMillis();
    }

    /**
     * @return Returns the totalCount.
     */
    public int getTotalCount() {
        return totalCount;
    }

    /**
     * @param totalCount The totalCount to set.
     */
    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }
}
