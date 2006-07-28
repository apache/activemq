/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Poor mans way of getting JUnit to run a test case through a few different
 * combinations of options.
 * 
 * 
 * Usage: If you have a test case called testFoo what you want to run through a
 * few combinations, of of values for the attributes age and color, you would
 * something like: <code>
 *    public void initCombosForTestFoo() {    
 *        addCombinationValues( "age", new Object[]{ new Integer(21), new Integer(30) } );
 *        addCombinationValues( "color", new Object[]{"blue", "green"} );
 *    }
 * </code>
 * 
 * The testFoo test case would be run for each possible combination of age and
 * color that you setup in the initCombosForTestFoo method. Before each combination is
 * run, the age and color fields of the test class are set to one of the values
 * defined. This is done before the normal setUp method is called.
 * 
 * If you want the test combinations to show up as separate test runs in the
 * JUnit reports, add a suite method to your test case similar to:
 * 
 * <code>
 *     public static Test suite() {
 *         return suite(FooTest.class);
 *     }
 * </code>
 * 
 * @version $Revision: 1.5 $
 */
public abstract class CombinationTestSupport extends AutoFailTestSupport {

    protected static final Log log = LogFactory.getLog(CombinationTestSupport.class);
    
    private HashMap comboOptions = new HashMap();
    private boolean combosEvaluated;
    private Map options;

    static class ComboOption {
        final String attribute;
        final LinkedHashSet values = new LinkedHashSet();

        public ComboOption(String attribute, Collection options) {
            this.attribute = attribute;
            this.values.addAll(options);
        }
    }

    public void addCombinationValues(String attribute, Object[] options) {
        ComboOption co = (ComboOption) this.comboOptions.get(attribute);
        if (co == null) {
            this.comboOptions.put(attribute, new ComboOption(attribute, Arrays.asList(options)));
        } else {
            co.values.addAll(Arrays.asList(options));
        }
    }

    public void runBare() throws Throwable {
        if (combosEvaluated) {
            super.runBare();
        } else {
            CombinationTestSupport[] combinations = getCombinations();
            for (int i = 0; i < combinations.length; i++) {
                CombinationTestSupport test = combinations[i];
                log.info("Running " + test.getName()); 
                test.runBare();
            }
        }
    }

    private void setOptions(Map options) throws NoSuchFieldException, IllegalAccessException {
        this.options = options;
        for (Iterator iterator = options.keySet().iterator(); iterator.hasNext();) {
            String attribute = (String) iterator.next();
            Object value = options.get(attribute);
            try {
                Field field = getClass().getField(attribute);
                field.set(this, value);
            } catch (Throwable e) {
                log.info("Could not set field '" + attribute + "' to value '" + value
                        + "', make sure the field exists and is public.");
            }
        }
    }

    private CombinationTestSupport[] getCombinations() {
        try {
            Method method = getClass().getMethod("initCombos", null);
            method.invoke(this, null);
        } catch (Throwable e) {
        }
        
        String name = getName();
        String comboSetupMethodName = "initCombosFor" + Character.toUpperCase(name.charAt(0)) + name.substring(1);
        try {
            Method method = getClass().getMethod(comboSetupMethodName, null);
            method.invoke(this, null);
        } catch (Throwable e) {
        }

        try {
            ArrayList expandedOptions = new ArrayList();
            expandCombinations(new ArrayList(comboOptions.values()), expandedOptions);
    
            if (expandedOptions.isEmpty()) {
                combosEvaluated = true;
                return new CombinationTestSupport[] { this };
            } else {
    
                ArrayList result = new ArrayList();
                // Run the test case for each possible combination
                for (Iterator iter = expandedOptions.iterator(); iter.hasNext();) {
                    CombinationTestSupport combo = (CombinationTestSupport) TestSuite.createTest(getClass(), getName());
                    combo.combosEvaluated = true;
                    combo.setOptions((Map) iter.next());
                    result.add(combo);
                }
    
                CombinationTestSupport rc[] = new CombinationTestSupport[result.size()];
                result.toArray(rc);
                return rc;
            }
        } catch (Throwable e) {
            combosEvaluated = true;
            return new CombinationTestSupport[] { this };
        }

    }

    private void expandCombinations(List optionsLeft, List expandedCombos) {
        if (!optionsLeft.isEmpty()) {
            HashMap map;
            if (comboOptions.size() == optionsLeft.size()) {
                map = new HashMap();
                expandedCombos.add(map);
            } else {
                map = (HashMap) expandedCombos.get(expandedCombos.size() - 1);
            }

            LinkedList l = new LinkedList(optionsLeft);
            ComboOption comboOption = (ComboOption) l.removeLast();
            int i = 0;
            for (Iterator iter = comboOption.values.iterator(); iter.hasNext();) {
                Object value = (Object) iter.next();
                if (i != 0) {
                    map = new HashMap(map);
                    expandedCombos.add(map);
                }
                map.put(comboOption.attribute, value);
                expandCombinations(l, expandedCombos);
                i++;
            }
        }
    }

    public static Test suite(Class clazz) {
        TestSuite suite = new TestSuite();

        ArrayList names = new ArrayList();
        Method[] methods = clazz.getMethods();
        for (int i = 0; i < methods.length; i++) {
            String name = methods[i].getName();
            if (names.contains(name) || !isPublicTestMethod(methods[i]))
                continue;
            names.add(name);
            Test test = TestSuite.createTest(clazz, name);
            if (test instanceof CombinationTestSupport) {
                CombinationTestSupport[] combinations = ((CombinationTestSupport) test).getCombinations();
                for (int j = 0; j < combinations.length; j++) {
                    suite.addTest(combinations[j]);
                }
            } else {
                suite.addTest(test);
            }
        }
        return suite;
    }

    static private boolean isPublicTestMethod(Method m) {
        return isTestMethod(m) && Modifier.isPublic(m.getModifiers());
    }

    static private boolean isTestMethod(Method m) {
        String name = m.getName();
        Class[] parameters = m.getParameterTypes();
        Class returnType = m.getReturnType();
        return parameters.length == 0 && name.startsWith("test") && returnType.equals(Void.TYPE);
    }

    public String getName() {
        if (options != null) {
            return super.getName() + " " + options;
        }
        return super.getName();
    }
}
