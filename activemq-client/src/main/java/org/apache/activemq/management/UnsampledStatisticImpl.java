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

import java.util.concurrent.atomic.AtomicReference;

/**
 * UnsampledStatistic<T> implementation
 * 
 */
public class UnsampledStatisticImpl<T> extends StatisticImpl implements UnsampledStatistic<T> {

    private final AtomicReference<T> value = new AtomicReference<>();
    private final T defaultValue;

    public UnsampledStatisticImpl(String name, String unit, String description, T defaultValue) {
        super(name, unit, description, 0l, 0l);
        this.value.set(defaultValue);
        this.defaultValue = defaultValue;
    }

    @Override
    public void reset() {
        if (isDoReset()) {
            value.set(defaultValue);
        }
    }

    @Override
    protected void updateSampleTime() {}

    @Override
    public long getStartTime() {
        return 0l;
    }

    @Override
    public long getLastSampleTime() {
        return 0l;
    }

    @Override
    public T getValue() {
        return value.get();
    }

    @Override
    public void setValue(T value) {
        if (isEnabled()) {
            this.value.set(value); 
        }
    }

    protected void appendFieldDescription(StringBuffer buffer) {
        buffer.append(" value: ");
        buffer.append(value.get());
        super.appendFieldDescription(buffer);
    }
}
