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
package org.apache.activemq.tool.reports;

import java.util.List;
import java.util.Iterator;

public final class PerformanceStatisticsUtil {
    private PerformanceStatisticsUtil() {
    }

    public static long getTotalThroughput(List totalTPList) {
        long totalTP = 0;
        if (totalTPList != null) {
            for (Iterator i=totalTPList.iterator(); i.hasNext();) {
                totalTP += ((Long)i.next()).longValue();
            }
        } else {
            totalTP = -1;
        }
        return totalTP;
    }

    public static long getMinThroughput(List totalTPList) {
        long minTP = Long.MAX_VALUE;
        if (totalTPList != null) {
            for (Iterator i=totalTPList.iterator(); i.hasNext();) {
                minTP = Math.min(((Long)i.next()).longValue(), minTP);
            }
        } else {
            minTP = -1;
        }
        return minTP;
    }

    public static long getMaxThroughput(List totalTPList) {
        long maxTP = Long.MIN_VALUE;
        if (totalTPList != null) {
            for (Iterator i=totalTPList.iterator(); i.hasNext();) {
                maxTP = Math.max(((Long)i.next()).longValue(), maxTP);
            }
        } else {
            maxTP = -1;
        }
        return maxTP;
    }

    public static double getAveThroughput(List totalTPList) {
        double aveTP;
        if (totalTPList != null) {
            int sampleCount = 0;
            long totalTP = 0;
            for (Iterator i=totalTPList.iterator(); i.hasNext();) {
                sampleCount++;
                totalTP += ((Long)i.next()).longValue();
            }
            return (double)totalTP / (double)sampleCount;
        } else {
            aveTP = -1;
        }
        return aveTP;
    }

    public static double getAveThroughputExcludingMinMax(List totalTPList) {
        double aveTP;
        long minTP = getMinThroughput(totalTPList);
        long maxTP = getMaxThroughput(totalTPList);
        if (totalTPList != null) {
            int sampleCount = 0;
            long totalTP = 0;
            long sampleTP;
            for (Iterator i=totalTPList.iterator(); i.hasNext();) {
                sampleCount++;
                sampleTP = ((Long)i.next()).longValue();
                if (sampleTP != minTP && sampleTP != maxTP) {
                    totalTP += sampleTP;
                }
            }
            return (double)totalTP / (double)sampleCount;
        } else {
            aveTP = -1;
        }
        return aveTP;
    }

}
