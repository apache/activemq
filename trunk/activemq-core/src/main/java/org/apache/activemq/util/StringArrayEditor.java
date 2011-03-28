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
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


public class StringArrayEditor extends PropertyEditorSupport {

    public void setAsText(String text) {
        if (text == null || text.length() == 0) {
            setValue(null);
        } else {
            StringTokenizer stok = new StringTokenizer(text, ",");
            final List<String> list = new ArrayList<String>();

            while (stok.hasMoreTokens()) {
                list.add(stok.nextToken());
            }

            Object array = list.toArray(new String[list.size()]);

            setValue(array);
        }
    }

    public String getAsText() {
        Object[] objects = (Object[]) getValue();
        if (objects == null || objects.length == 0) {
            return null;
        }

        StringBuffer result = new StringBuffer(String.valueOf(objects[0]));
        for (int i = 1; i < objects.length; i++) {
            result.append(",").append(objects[i]);
        }

        return result.toString();

    }

}

