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
package org.apache.activemq.xbean.sample;

/**
 * The bottom of the hierarchy; not tagged, so never an element, but its
 * setters still become attributes of the tagged subclasses.
 */
public class RootBean implements Runnable {
    private String rootValue;

    /**
     * The value inherited from the root of the hierarchy.
     */
    public void setRootValue(String rootValue) {
        this.rootValue = rootValue;
    }

    public String getRootValue() {
        return rootValue;
    }

    /**
     * @org.apache.xbean.InitMethod
     */
    public void inheritedInit() {
    }

    @Override
    public void run() {
    }
}
