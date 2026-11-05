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

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A bean with a bit of everything. Links like {@link RootBean} and HTML such
 * as <a href="https://activemq.apache.org">this</a> stay in the description.
 *
 * Paragraphs are separated by blank lines and an indented example keeps its
 * indentation minus the one space after the asterisk:
 *
 * <pre class="code">
 * <bean id="plain" class="org.apache.activemq.xbean.sample.PlainBean">
 *   <property name="name" value="x" />
 * </bean>
 * </pre>
 *
 * @org.apache.xbean.XBean rootElement="true"
 */
public class PlainBean extends BaseBean {
    private String name;
    private boolean enabled;
    private String DLQ;
    private String secret;
    private List<String> items;
    private List plainList;
    private String[] names;
    private int[][] grid;
    private File directory;
    private Map<String, String> settings;
    private int overridden;
    private int size;

    public PlainBean() {
    }

    /**
     * Builds a bean with a name.
     *
     * @param name the name to use, which is also a property
     * @param size how many things it holds; also a property, whose own description wins
     */
    public PlainBean(String name, int size) {
        this.name = name;
        this.size = size;
    }

    /**
     * Copies everything from properties.
     *
     * @param properties every setting, copied; not a property either
     */
    public PlainBean(Properties properties) {
    }

    /**
     * Sets the name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * The name of this bean; the getter comment wins over the setter comment.
     */
    public String getName() {
        return name;
    }

    /** Whether the bean is enabled */
    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /** Introspector keeps the capitals of an acronym */
    public void setDLQ(String DLQ) {
        this.DLQ = DLQ;
    }

    /** A private setter still counts as a property */
    private void setSecret(String secret) {
        this.secret = secret;
    }

    /** Static methods are not properties */
    public static void setIgnored(String ignored) {
    }

    /**
     * A list whose child type is named by the tag on a continuation line.
     *
     * @org.apache.xbean.Property
     *                            nestedType="org.apache.activemq.xbean.sample.Widget"
     */
    public void setItems(List<String> items) {
        this.items = items;
    }

    /** A raw list with no nested type falls back to java.lang.Object */
    public void setPlainList(List plainList) {
        this.plainList = plainList;
    }

    /** An array of strings */
    public void setNames(String[] names) {
        this.names = names;
    }

    public void setGrid(int[][] grid) {
        this.grid = grid;
    }

    /** A File has no editor on the default search path, so it is a nested element */
    public void setDirectory(File directory) {
        this.directory = directory;
    }

    /** Maps are unbounded nested elements */
    public void setSettings(Map<String, String> settings) {
        this.settings = settings;
    }

    /** The subclass setter is the one that counts */
    public void setOverridden(int overridden) {
        this.overridden = overridden;
    }

    /**
     * @org.apache.xbean.Property propertyEditor="org.apache.activemq.xbean.sample.SizeEditor"
     */
    public void setSize(int size) {
        this.size = size;
    }

    /**
     * @org.apache.xbean.Property hidden="true"
     */
    public void setHiddenValue(String hiddenValue) {
    }

    /**
     * @org.apache.xbean.Property alias="renamed" description="An aliased attribute"
     */
    public void setOriginal(String original) {
    }

    /** Only a getter: not an attribute */
    public int getCount() {
        return 0;
    }

    /**
     * @org.apache.xbean.InitMethod
     */
    public void start() {
    }

    /**
     * @org.apache.xbean.DestroyMethod
     */
    public void stop() {
    }

    /** Nested and tagged, but nested types are never elements. */
    public static class Nested {
        /**
         * @org.apache.xbean.XBean element="nestedShouldNotAppear"
         */
        public void setIgnored(String value) {
        }
    }
}
