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
package org.apache.activemq.ra;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class ActiveMQResourceAdapterJavaBeanEqualityTest {

    private ActiveMQResourceAdapter raOne;
    private ActiveMQResourceAdapter raTwo;

    @Before
    public void setUp() throws Exception {
        raOne = new ActiveMQResourceAdapter();
        raTwo = new ActiveMQResourceAdapter();
    }

    @Test(timeout = 60000)
    public void testSelfEquality() {
        assertEquality(raOne, raOne);
    }

    @Test(timeout = 60000)
    public void testEmptyEquality() {
        assertEquality(raOne, raTwo);
    }

    @Test(timeout = 60000)
    public void testNullEqualityFailure() {
        assertFalse(raOne.equals(null));
    }

    @Test(timeout = 60000)
    public void testServerUrlEquality() {
        raOne.setServerUrl("one");
        raTwo.setServerUrl("one");
        assertEquality(raOne, raTwo);
    }

    @Test(timeout = 60000)
    public void testServerUrlInequality() {
        raOne.setServerUrl("one");
        raTwo.setServerUrl("two");
        assertNonEquality(raOne, raTwo);
    }

    @Test(timeout = 60000)
    public void testServerUrlInequalityDifferentCase() {
        raOne.setServerUrl("one");
        raTwo.setServerUrl("ONE");
        assertNonEquality(raOne, raTwo);
    }

    @Test(timeout = 60000)
    public void testNullServerUrlInequality() {
        raOne.setServerUrl("one");
        raTwo.setServerUrl(null);
        assertNonEquality(raOne, raTwo);
    }

    @Test(timeout = 60000)
    public void testBrokerXMLConfigEquality() {
        raOne.setBrokerXmlConfig("one");
        raTwo.setBrokerXmlConfig("one");
        assertEquality(raOne, raTwo);
    }

    @Test(timeout = 60000)
    public void testBrokerXMLConfigInequality() {
        raOne.setBrokerXmlConfig("one");
        raTwo.setBrokerXmlConfig("two");
        assertNonEquality(raOne, raTwo);
    }

    @Test(timeout = 60000)
    public void testBrokerXMLConfigInequalityDifferentCase() {
        raOne.setBrokerXmlConfig("one");
        raTwo.setBrokerXmlConfig("ONE");
        assertNonEquality(raOne, raTwo);
    }

    @Test(timeout = 60000)
    public void testNullBrokerXMLConfigInequality() {
        raOne.setBrokerXmlConfig("one");
        raTwo.setBrokerXmlConfig(null);
        assertNonEquality(raOne, raTwo);
    }

    @Test(timeout = 60000)
    public void testPasswordNotPartOfEquality() {
        raOne.setClientid("one");
        raTwo.setClientid("one");
        raOne.setPassword("foo");
        raTwo.setPassword("bar");
        assertEquality(raOne, raTwo);
    }

    private void assertEquality(ActiveMQResourceAdapter leftRa, ActiveMQResourceAdapter rightRa) {
        assertTrue("ActiveMQResourceAdapters are not equal", leftRa.equals(rightRa));
        assertTrue("ActiveMQResourceAdapters are not equal", rightRa.equals(leftRa));
        assertTrue("HashCodes are not equal", leftRa.hashCode() == rightRa.hashCode());
    }

    private void assertNonEquality(ActiveMQResourceAdapter leftRa, ActiveMQResourceAdapter rightRa) {
        assertFalse("ActiveMQResourceAdapters are equal", leftRa.equals(rightRa));
        assertFalse("ActiveMQResourceAdapters are equal", rightRa.equals(leftRa));
        assertFalse("HashCodes are equal", leftRa.hashCode() == rightRa.hashCode());
    }
}
