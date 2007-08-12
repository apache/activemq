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

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.util.Arrays;
import java.util.List;

import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.resource.spi.InvalidPropertyException;

import junit.framework.TestCase;
import org.apache.activemq.command.ActiveMQDestination;

/**
 * @version $Revision$
 */
public class ActiveMQActivationSpecTest extends TestCase {

    private static final String DESTINATION = "defaultQueue";
    private static final String DESTINATION_TYPE = Queue.class.getName();
    private static final String EMPTY_STRING = "   ";

    private ActiveMQActivationSpec activationSpec;
    private PropertyDescriptor destinationProperty;
    private PropertyDescriptor destinationTypeProperty;
    private PropertyDescriptor acknowledgeModeProperty;
    private PropertyDescriptor subscriptionDurabilityProperty;
    private PropertyDescriptor clientIdProperty;
    private PropertyDescriptor subscriptionNameProperty;

    public ActiveMQActivationSpecTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();

        activationSpec = new ActiveMQActivationSpec();
        activationSpec.setDestination(DESTINATION);
        activationSpec.setDestinationType(DESTINATION_TYPE);

        destinationProperty = new PropertyDescriptor("destination", ActiveMQActivationSpec.class);
        destinationTypeProperty = new PropertyDescriptor("destinationType", ActiveMQActivationSpec.class);
        acknowledgeModeProperty = new PropertyDescriptor("acknowledgeMode", ActiveMQActivationSpec.class);
        subscriptionDurabilityProperty = new PropertyDescriptor("subscriptionDurability", ActiveMQActivationSpec.class);
        clientIdProperty = new PropertyDescriptor("clientId", ActiveMQActivationSpec.class);
        subscriptionNameProperty = new PropertyDescriptor("subscriptionName", ActiveMQActivationSpec.class);
    }

    public void testDefaultContructionValidation() throws IntrospectionException {
        PropertyDescriptor[] expected = {destinationTypeProperty, destinationProperty};
        assertActivationSpecInvalid(new ActiveMQActivationSpec(), expected);
    }

    public void testMinimalSettings() {
        assertEquals(DESTINATION, activationSpec.getDestination());
        assertEquals(DESTINATION_TYPE, activationSpec.getDestinationType());
        assertActivationSpecValid();
    }

    public void testNoDestinationTypeFailure() {
        activationSpec.setDestinationType(null);
        PropertyDescriptor[] expected = {destinationTypeProperty};
        assertActivationSpecInvalid(expected);
    }

    public void testInvalidDestinationTypeFailure() {
        activationSpec.setDestinationType("foobar");
        PropertyDescriptor[] expected = {destinationTypeProperty};
        assertActivationSpecInvalid(expected);
    }

    public void testQueueDestinationType() {
        activationSpec.setDestinationType(Queue.class.getName());
        assertActivationSpecValid();
    }

    public void testTopicDestinationType() {
        activationSpec.setDestinationType(Topic.class.getName());
        assertActivationSpecValid();
    }

    public void testSuccessfulCreateQueueDestination() {
        activationSpec.setDestinationType(Queue.class.getName());
        activationSpec.setDestination(DESTINATION);
        assertActivationSpecValid();
        ActiveMQDestination destination = activationSpec.createDestination();
        assertNotNull("ActiveMQDestination not created", destination);
        assertEquals("Physical name not the same", activationSpec.getDestination(), destination.getPhysicalName());
        assertTrue("Destination is not a Queue", destination instanceof Queue);
    }

    public void testSuccessfulCreateTopicDestination() {
        activationSpec.setDestinationType(Topic.class.getName());
        activationSpec.setDestination(DESTINATION);
        assertActivationSpecValid();
        ActiveMQDestination destination = activationSpec.createDestination();
        assertNotNull("ActiveMQDestination not created", destination);
        assertEquals("Physical name not the same", activationSpec.getDestination(), destination.getPhysicalName());
        assertTrue("Destination is not a Topic", destination instanceof Topic);
    }

    public void testCreateDestinationIncorrectType() {
        activationSpec.setDestinationType(null);
        activationSpec.setDestination(DESTINATION);
        ActiveMQDestination destination = activationSpec.createDestination();
        assertNull("ActiveMQDestination should not have been created", destination);
    }

    public void testCreateDestinationIncorrectDestinationName() {
        activationSpec.setDestinationType(Topic.class.getName());
        activationSpec.setDestination(null);
        ActiveMQDestination destination = activationSpec.createDestination();
        assertNull("ActiveMQDestination should not have been created", destination);
    }

//----------- acknowledgeMode tests
    public void testDefaultAcknowledgeModeSetCorrectly() {
        assertEquals("Incorrect default value", ActiveMQActivationSpec.AUTO_ACKNOWLEDGE_MODE,
                activationSpec.getAcknowledgeMode());
        assertEquals("Incorrect default value", Session.AUTO_ACKNOWLEDGE,
                activationSpec.getAcknowledgeModeForSession());
    }

    public void testInvalidAcknowledgeMode() {
        activationSpec.setAcknowledgeMode("foobar");
        PropertyDescriptor[] expected = {acknowledgeModeProperty};
        assertActivationSpecInvalid(expected);
        assertEquals("Incorrect acknowledge mode", ActiveMQActivationSpec.INVALID_ACKNOWLEDGE_MODE, 
                activationSpec.getAcknowledgeModeForSession());
    }

    public void testNoAcknowledgeMode() {
        activationSpec.setAcknowledgeMode(null);
        PropertyDescriptor[] expected = {acknowledgeModeProperty};
        assertActivationSpecInvalid(expected);
        assertEquals("Incorrect acknowledge mode", ActiveMQActivationSpec.INVALID_ACKNOWLEDGE_MODE,
                activationSpec.getAcknowledgeModeForSession());
    }

    public void testSettingAutoAcknowledgeMode() {
        activationSpec.setAcknowledgeMode(ActiveMQActivationSpec.AUTO_ACKNOWLEDGE_MODE);
        assertActivationSpecValid();
        assertEquals("Incorrect acknowledge mode", Session.AUTO_ACKNOWLEDGE,
                activationSpec.getAcknowledgeModeForSession());
    }

    public void testSettingDupsOkAcknowledgeMode() {
        activationSpec.setAcknowledgeMode(ActiveMQActivationSpec.DUPS_OK_ACKNOWLEDGE_MODE);
        assertActivationSpecValid();
        assertEquals("Incorrect acknowledge mode", Session.DUPS_OK_ACKNOWLEDGE,
                activationSpec.getAcknowledgeModeForSession());
    }

//----------- subscriptionDurability tests
    public void testDefaultSubscriptionDurabilitySetCorrectly() {
        assertEquals("Incorrect default value", ActiveMQActivationSpec.NON_DURABLE_SUBSCRIPTION,
                activationSpec.getSubscriptionDurability());
    }
    
    public void testInvalidSubscriptionDurability() {
        activationSpec.setSubscriptionDurability("foobar");
        PropertyDescriptor[] expected = {subscriptionDurabilityProperty};
        assertActivationSpecInvalid(expected);
    }

    public void testNullSubscriptionDurability() {
        activationSpec.setSubscriptionDurability(null);
        PropertyDescriptor[] expected = {subscriptionDurabilityProperty};
        assertActivationSpecInvalid(expected);
    }

    public void testSettingNonDurableSubscriptionDurability() {
        activationSpec.setSubscriptionDurability(ActiveMQActivationSpec.NON_DURABLE_SUBSCRIPTION);
        assertActivationSpecValid();
    }

//----------- durable subscriber tests    
    public void testValidDurableSubscriber() {
        activationSpec.setDestinationType(Topic.class.getName());
        activationSpec.setSubscriptionDurability(ActiveMQActivationSpec.DURABLE_SUBSCRIPTION);
        activationSpec.setClientId("foobar");
        activationSpec.setSubscriptionName("foobar");
        assertActivationSpecValid();        
        assertTrue(activationSpec.isDurableSubscription());
    }

    public void testDurableSubscriberWithQueueDestinationTypeFailure() {
        activationSpec.setDestinationType(Queue.class.getName());
        activationSpec.setSubscriptionDurability(ActiveMQActivationSpec.DURABLE_SUBSCRIPTION);
        activationSpec.setClientId("foobar");
        activationSpec.setSubscriptionName("foobar");
        PropertyDescriptor[] expected = {subscriptionDurabilityProperty};
        assertActivationSpecInvalid(expected);
    }
    
    public void testDurableSubscriberNoClientIdNoSubscriptionNameFailure() {
        activationSpec.setDestinationType(Topic.class.getName());
        activationSpec.setSubscriptionDurability(ActiveMQActivationSpec.DURABLE_SUBSCRIPTION);
        activationSpec.setClientId(null);
        assertNull(activationSpec.getClientId());
        activationSpec.setSubscriptionName(null);
        assertNull(activationSpec.getSubscriptionName());
        PropertyDescriptor[] expected = {clientIdProperty, subscriptionNameProperty};
        assertActivationSpecInvalid(expected);
    }    
    
    public void testDurableSubscriberEmptyClientIdEmptySubscriptionNameFailure() {
        activationSpec.setDestinationType(Topic.class.getName());
        activationSpec.setSubscriptionDurability(ActiveMQActivationSpec.DURABLE_SUBSCRIPTION);
        activationSpec.setClientId(EMPTY_STRING);
        assertNull(activationSpec.getClientId());
        activationSpec.setSubscriptionName(EMPTY_STRING);
        assertNull(activationSpec.getSubscriptionName());
        PropertyDescriptor[] expected = {clientIdProperty, subscriptionNameProperty};
        assertActivationSpecInvalid(expected);
    }    
    
    public void testSetEmptyStringButGetNullValue() {
        ActiveMQActivationSpec activationSpec = new ActiveMQActivationSpec();
        
        activationSpec.setDestinationType(EMPTY_STRING);
        assertNull("Property not null", activationSpec.getDestinationType());
        
        activationSpec.setMessageSelector(EMPTY_STRING);
        assertNull("Property not null", activationSpec.getMessageSelector());
        
        activationSpec.setDestination(EMPTY_STRING);
        assertNull("Property not null", activationSpec.getDestination());
        
        activationSpec.setUserName(EMPTY_STRING);
        assertNull("Property not null", activationSpec.getUserName());
        
        activationSpec.setPassword(EMPTY_STRING);
        assertNull("Property not null", activationSpec.getPassword());
        
        activationSpec.setClientId(EMPTY_STRING);
        assertNull("Property not null", activationSpec.getClientId());
        
        activationSpec.setSubscriptionName(EMPTY_STRING);
        assertNull("Property not null", activationSpec.getSubscriptionName());        
    }

//----------- helper methods    
    private void assertActivationSpecValid() {
        try {
            activationSpec.validate();
        } catch (InvalidPropertyException e) {
            fail("InvalidPropertyException should not be thrown");
        }
    }
    
    private void assertActivationSpecInvalid(PropertyDescriptor[] expected) {
        assertActivationSpecInvalid(activationSpec, expected);
    }

    private void assertActivationSpecInvalid(ActiveMQActivationSpec testActivationSpec, PropertyDescriptor[] expected) {
        try {
            testActivationSpec.validate();
            fail("InvalidPropertyException should have been thrown");
        } catch (InvalidPropertyException e) {
            PropertyDescriptor[] actual = e.getInvalidPropertyDescriptors();
            assertEquals(expected, actual);
        }
    }

    private static void assertEquals(PropertyDescriptor[] expected, PropertyDescriptor[] actual) {
        /*
        * This is kind of ugly.  I originally created two HashSets and did an assertEquals(set1, set2) 
        * but because of a bug in the PropertyDescriptor class, it incorrectly fails.  The problem is that the 
        * PropertyDescriptor class implements the equals() method but not the hashCode() method and almost all
        * of the java collection classes use hashCode() for testing equality.  The one exception I found was
        * the ArrayList class which uses equals() for testing equality.  Since Arrays.asList(...) returns an
        * ArrayList, I use it below.  Yes, ugly ... I know.
        *
        * see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4634390
        */
        assertNotNull("No PropertyDescriptors returned", expected);
        assertEquals("PropertyDescriptor array size is incorrect ", expected.length, actual.length);
        List<PropertyDescriptor> expectedList = Arrays.asList(expected);
        List<PropertyDescriptor> actualList = Arrays.asList(actual);
        assertTrue("Incorrect PropertyDescriptors returned", expectedList.containsAll(actualList));
    }
    
    public void testSelfEquality() {
        assertEquality(activationSpec, activationSpec);
    }

    public void testSamePropertiesButNotEqual() {
        assertNonEquality(new ActiveMQActivationSpec(), new ActiveMQActivationSpec());
    }

    private void assertEquality(ActiveMQActivationSpec leftSpec, ActiveMQActivationSpec rightSpec) {
        assertTrue("ActiveMQActivationSpecs are not equal", leftSpec.equals(rightSpec));
        assertTrue("ActiveMQActivationSpecs are not equal", rightSpec.equals(leftSpec));
        assertTrue("HashCodes are not equal", leftSpec.hashCode() == rightSpec.hashCode());
    }
    
    private void assertNonEquality(ActiveMQActivationSpec leftSpec, ActiveMQActivationSpec rightSpec) {
        assertFalse("ActiveMQActivationSpecs are equal", leftSpec.equals(rightSpec));
        assertFalse("ActiveMQActivationSpecs are equal", rightSpec.equals(leftSpec));
        assertFalse("HashCodes are equal", leftSpec.hashCode() == rightSpec.hashCode());
    }
    
}
