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
package org.apache.activemq.management;


/**
 * A bounded range statistic implementation
 *
 * 
 */
public class BoundedRangeStatisticImpl extends RangeStatisticImpl {
    private long lowerBound;
    private long upperBound;

    public BoundedRangeStatisticImpl(String name, String unit, String description, long lowerBound, long upperBound) {
        super(name, unit, description);
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public long getLowerBound() {
        return lowerBound;
    }

    public long getUpperBound() {
        return upperBound;
    }

    protected void appendFieldDescription(StringBuffer buffer) {
        buffer.append(" lowerBound: ");
        buffer.append(Long.toString(lowerBound));
        buffer.append(" upperBound: ");
        buffer.append(Long.toString(upperBound));
        super.appendFieldDescription(buffer);
    }
}
