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
package org.apache.activemq.tool;

import junit.framework.TestCase;

import java.util.Properties;

public class ReflectionUtilTest extends TestCase {
    private boolean booleanData;
    private int     intData;
    private long    longData;
    private short   shortData;
    private double  doubleData;
    private float   floatData;
    private byte    byteData;
    private char    charData;
    private String  stringData;
    private ReflectionUtilTest testData;

    public void testDataTypeConfig() {
        // Initialize variables;
        booleanData = false;
        intData     = 0;
        longData    = 0;
        shortData   = 0;
        doubleData  = 0.0;
        floatData   = 0.0F;
        byteData    = 0;
        charData    = '0';
        stringData  = "false";

        Object targetObj = new ReflectionUtilTest();

        // Set properties
        Properties props = new Properties();
        props.setProperty("test.booleanData", "true");
        props.setProperty("test.intData", "1000");
        props.setProperty("test.longData", "2000");
        props.setProperty("test.shortData", "3000");
        props.setProperty("test.doubleData", "1234.567");
        props.setProperty("test.floatData", "9876.543");
        props.setProperty("test.byteData", "127");
        props.setProperty("test.charData", "A");
        props.setProperty("test.stringData", "true");
        props.setProperty("test.testData", "TEST.FOO.BAR");

        ReflectionUtil.configureClass(targetObj, props);

        // Check config
        assertEquals(true, ((ReflectionUtilTest)targetObj).isBooleanData());
        assertEquals(1000, ((ReflectionUtilTest)targetObj).getIntData());
        assertEquals(2000, ((ReflectionUtilTest)targetObj).getLongData());
        assertEquals(3000, ((ReflectionUtilTest)targetObj).getShortData());
        assertEquals(1234.567, ((ReflectionUtilTest)targetObj).getDoubleData(), 0.0001);
        assertEquals(9876.543, ((ReflectionUtilTest)targetObj).getFloatData(), 0.0001);
        assertEquals(127, ((ReflectionUtilTest)targetObj).getByteData());
        assertEquals('A', ((ReflectionUtilTest)targetObj).getCharData());
        assertEquals("true", ((ReflectionUtilTest)targetObj).getStringData());
        assertEquals("TEST.FOO.BAR", ((ReflectionUtilTest)targetObj).getTestData().getStringData());
    }

    public void testNestedConfig() {
        ReflectionUtilTest t1 = new ReflectionUtilTest();
        ReflectionUtilTest t2 = new ReflectionUtilTest();
        ReflectionUtilTest t3 = new ReflectionUtilTest();
        ReflectionUtilTest t4 = new ReflectionUtilTest();
        ReflectionUtilTest t5 = new ReflectionUtilTest();

        ReflectionUtil.configureClass(t1, "test.stringData", "t1");
        assertEquals("t1", t1.getStringData());

        t1.setTestData(t2);
        ReflectionUtil.configureClass(t1, "test.testData.stringData", "t2");
        assertEquals("t2", t2.getStringData());

        t2.setTestData(t3);
        ReflectionUtil.configureClass(t1, "test.testData.testData.stringData", "t3");
        assertEquals("t3", t3.getStringData());

        t3.setTestData(t4);
        ReflectionUtil.configureClass(t1, "test.testData.testData.testData.stringData", "t4");
        assertEquals("t4", t4.getStringData());

        t4.setTestData(t5);
        ReflectionUtil.configureClass(t1, "test.testData.testData.testData.testData.stringData", "t5");
        assertEquals("t5", t5.getStringData());
    }

    public boolean isBooleanData() {
        return booleanData;
    }

    public void setBooleanData(boolean booleanData) {
        this.booleanData = booleanData;
    }

    public int getIntData() {
        return intData;
    }

    public void setIntData(int intData) {
        this.intData = intData;
    }

    public long getLongData() {
        return longData;
    }

    public void setLongData(long longData) {
        this.longData = longData;
    }

    public short getShortData() {
        return shortData;
    }

    public void setShortData(short shortData) {
        this.shortData = shortData;
    }

    public double getDoubleData() {
        return doubleData;
    }

    public void setDoubleData(double doubleData) {
        this.doubleData = doubleData;
    }

    public float getFloatData() {
        return floatData;
    }

    public void setFloatData(float floatData) {
        this.floatData = floatData;
    }

    public byte getByteData() {
        return byteData;
    }

    public void setByteData(byte byteData) {
        this.byteData = byteData;
    }

    public char getCharData() {
        return charData;
    }

    public void setCharData(char charData) {
        this.charData = charData;
    }

    public String getStringData() {
        return stringData;
    }

    public void setStringData(String stringData) {
        this.stringData = stringData;
    }

    public ReflectionUtilTest getTestData() {
        return testData;
    }

    public void setTestData(ReflectionUtilTest testData) {
        this.testData = testData;
    }

    public static ReflectionUtilTest valueOf(String data) {
        ReflectionUtilTest obj = new ReflectionUtilTest();
        obj.setStringData(data);
        return obj;
    }
}
