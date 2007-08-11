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
package org.apache.activemq.systest;

import org.springframework.context.ApplicationContext;

import javax.jms.Destination;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * A helper class for creating a test suite
 * 
 * @version $Revision: 1.1 $
 */
public class ScenarioTestSuite extends TestCase {

    private List brokers = new ArrayList();
    private boolean cacheBrokers;

    public static TestSuite createSuite(ApplicationContext clientContext, ApplicationContext brokerContext, Class[] scenarios, int destinationType) throws Exception {
        TestSuite suite = new TestSuite();

        ScenarioTestSuite test = new ScenarioTestSuite();
        test.appendTestCases(suite, clientContext, brokerContext, scenarios, destinationType);
        return suite;
    }

    public void appendTestCases(TestSuite suite, ApplicationContext clientContext, ApplicationContext brokerContext, Class[] scenarios, int destinationType) throws Exception {
        for (int i = 0; i < scenarios.length; i++) {
            Class scenario = scenarios[i];
            appendTestCase(suite, clientContext, brokerContext, scenario, destinationType);
        }
    }

    public void appendTestCase(TestSuite suite, ApplicationContext clientContext, ApplicationContext brokerContext, Class scenario, int destinationType) throws Exception {
        // lets figure out how to create the scenario from all the options
        // available

        Constructor[] constructors = scenario.getConstructors();
        for (int i = 0; i < constructors.length; i++) {
            Constructor constructor = constructors[i];
            appendTestCase(suite, clientContext, brokerContext, scenario, constructor, destinationType);
        }
    }

    public void appendTestCase(TestSuite suite, ApplicationContext clientContext, ApplicationContext brokerContext, Class scenario, Constructor constructor,
            int destinationType) throws Exception {
        // lets configure the test case
        Class[] parameterTypes = constructor.getParameterTypes();
        int size = parameterTypes.length;
        String[][] namesForParameters = new String[size][];
        for (int i = 0; i < size; i++) {
            Class parameterType = parameterTypes[i];
            String[] names = clientContext.getBeanNamesForType(parameterType);
            if (names == null || names.length == 0) {
                if (parameterType.equals(BrokerAgent.class)) {
                    names = new String[1];
                }
                else {
                    fail("No bean instances available in the ApplicationContext for type: " + parameterType.getName());
                }
            }
            namesForParameters[i] = names;
        }

        // lets try out each permutation of configuration
        int[] counters = new int[size];
        boolean completed = false;
        while (!completed) {
            Object[] parameters = new Object[size];
            StringBuffer buffer = new StringBuffer(scenario.getName());
            int brokerCounter = 1;
            for (int i = 0; i < size; i++) {
                String beanName = namesForParameters[i][counters[i]];
                if (beanName != null) {
                    parameters[i] = clientContext.getBean(beanName);
                    buffer.append(".");
                    buffer.append(beanName);
                }
                else {
                    parameters[i] = getBrokerAgent(brokerContext, brokerCounter++);
                }
            }

            String destinationName = buffer.toString();
            addTestsFor(suite, clientContext, brokerContext, constructor, parameters, destinationName, destinationType);

            // now lets count though the options
            int pivot = 0;
            while (++counters[pivot] >= namesForParameters[pivot].length) {
                counters[pivot] = 0;
                pivot++;
                if (pivot >= size) {
                    completed = true;
                    break;
                }
            }
        }
    }

    protected BrokerAgent getBrokerAgent(ApplicationContext brokerContext, int brokerCounter) {
        if (cacheBrokers) {
            BrokerAgent broker = null;
            int index = brokerCounter - 1;

            // lets reuse broker instances across test cases
            if (brokers.size() >= brokerCounter) {
                broker = (BrokerAgent) brokers.get(index);
            }
            if (broker == null) {
                broker = (BrokerAgent) brokerContext.getBean("broker" + brokerCounter);
                brokers.add(index, broker);
            }
            return broker;
        }
        else {
            return (BrokerAgent) brokerContext.getBean("broker" + brokerCounter);
        }
    }

    protected void addTestsFor(TestSuite suite, ApplicationContext clientContext, ApplicationContext brokerContext, Constructor constructor,
            Object[] parameters, String destinationName, int destinationType) throws Exception {
        Collection values = clientContext.getBeansOfType(DestinationFactory.class).values();
        assertTrue("we should at least one DestinationFactory in the ApplicationContext", values.size() > 0);

        for (Iterator iter = values.iterator(); iter.hasNext();) {
            DestinationFactory destinationFactory = (DestinationFactory) iter.next();

            Object instance = constructor.newInstance(parameters);
            assertTrue("Instance is not a Scenario: " + instance, instance instanceof Scenario);
            Scenario scenarioInstance = (Scenario) instance;

            String testName = destinationDescription(destinationType) + "." + destinationName;
            Destination destination = destinationFactory.createDestination(testName, destinationType);
            scenarioInstance.setDestination(destination);

            suite.addTest(new ScenarioTestCase(scenarioInstance, testName));
        }
    }

    public static String destinationDescription(int destinationType) {
        switch (destinationType) {
        case DestinationFactory.QUEUE:
            return "queue";

        case DestinationFactory.TOPIC:
            return "topic";

        default:
            return "Unknown";
        }
    }
}
