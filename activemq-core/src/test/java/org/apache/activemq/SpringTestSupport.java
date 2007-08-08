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

package org.apache.activemq;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.AbstractApplicationContext;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;

/**
 * A useful base class for spring based unit test cases
 * 
 * @version $Revision: 1.1 $
 */
public abstract class SpringTestSupport extends TestCase {

    protected final Log log = LogFactory.getLog(getClass());

    protected AbstractApplicationContext context;

    protected void setUp() throws Exception {
        context = createApplicationContext();
    }

    protected abstract AbstractApplicationContext createApplicationContext();;

    protected void tearDown() throws Exception {
        if (context != null) {
            context.destroy();
        }
    }

    protected Object getBean(String name) {
        Object bean = context.getBean(name);
        if (bean == null) {
            fail("Should have found bean named '" + name + "' in the Spring ApplicationContext");
        }
        return bean;
    }

    protected void assertSetEquals(String description, Object[] expected, Set actual) {
        Set expectedSet = new HashSet();
        expectedSet.addAll(Arrays.asList(expected));
        assertEquals(description, expectedSet, actual);
    }

}
