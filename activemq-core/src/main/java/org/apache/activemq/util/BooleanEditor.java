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
 * Used by xbean to set booleans.
 * <p/>
 * <b>Important: </b> Do not use this for other purposes than xbean, as property editors
 * are not thread safe, and they are slow to use.
 */
public class BooleanEditor extends PropertyEditorSupport {

    public String getJavaInitializationString() {
        return String.valueOf(((Boolean)getValue()).booleanValue());
    }

    public String getAsText() {
       return getJavaInitializationString();
    }

    public void setAsText(String text) throws java.lang.IllegalArgumentException {
        if (text.toLowerCase().equals("true")) {
            setValue(Boolean.TRUE);
        } else if (text.toLowerCase().equals("false")) {
            setValue(Boolean.FALSE);
        } else {
            throw new java.lang.IllegalArgumentException(text);
        }
    }

    public String[] getTags() {
        String result[] = { "true", "false" };
        return result;
    }
}
