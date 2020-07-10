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

import java.beans.PropertyEditorSupport;

/**
 * Used by xbean to set longs.
 * <p/>
 * <b>Important: </b> Do not use this for other purposes than xbean, as property editors
 * are not thread safe, and they are slow to use.
 * <p/>
 * Converts string values like "20 Mb", "1024kb", and "1g" to long values in
 * bytes.
 */
public class MemoryPropertyEditor extends PropertyEditorSupport {
    public void setAsText(String text) throws IllegalArgumentException {
        setValue(XBeanByteConverterUtil.convertToLongBytes(text));
    }

    public String getAsText() {
        Long value = (Long)getValue();
        return value != null ? value.toString() : "";
    }

}
