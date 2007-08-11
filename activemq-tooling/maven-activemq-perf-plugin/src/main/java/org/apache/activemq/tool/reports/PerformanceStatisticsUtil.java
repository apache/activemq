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
package org.apache.activemq.tool.reports;

import java.util.List;
import java.util.Iterator;

public final class PerformanceStatisticsUtil {
    private PerformanceStatisticsUtil() {
    }

    public static long getSum(List numList) {
        long sum = 0;
        if (numList != null) {
            for (Iterator i=numList.iterator(); i.hasNext();) {
                sum += ((Long)i.next()).longValue();
            }
        } else {
            sum = -1;
        }
        return sum;
    }

    public static long getMin(List numList) {
        long min = Long.MAX_VALUE;
        if (numList != null) {
            for (Iterator i=numList.iterator(); i.hasNext();) {
                min = Math.min(((Long)i.next()).longValue(), min);
            }
        } else {
            min = -1;
        }
        return min;
    }

    public static long getMax(List numList) {
        long max = Long.MIN_VALUE;
        if (numList != null) {
            for (Iterator i=numList.iterator(); i.hasNext();) {
                max = Math.max(((Long)i.next()).longValue(), max);
            }
        } else {
            max = -1;
        }
        return max;
    }

    public static double getAve(List numList) {
        double ave;
        if (numList != null) {
            int sampleCount = 0;
            long totalTP = 0;
            for (Iterator i=numList.iterator(); i.hasNext();) {
                sampleCount++;
                totalTP += ((Long)i.next()).longValue();
            }
            return (double)totalTP / (double)sampleCount;
        } else {
            ave = -1;
        }
        return ave;
    }

    public static double getAveEx(List numList) {
        double ave;
        long minTP = getMin(numList);
        long maxTP = getMax(numList);
        if (numList != null) {
            int sampleCount = 0;
            long totalTP = 0;
            long sampleTP;
            for (Iterator i=numList.iterator(); i.hasNext();) {
                sampleCount++;
                sampleTP = ((Long)i.next()).longValue();
                if (sampleTP != minTP && sampleTP != maxTP) {
                    totalTP += sampleTP;
                }
            }
            return (double)totalTP / (double)sampleCount;
        } else {
            ave = -1;
        }
        return ave;
    }

}
