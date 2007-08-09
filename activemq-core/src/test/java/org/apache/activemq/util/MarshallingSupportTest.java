/**
 * 
 */

package org.apache.activemq.util;

import java.util.Properties;

import junit.framework.TestCase;

/**
 * @author rajdavies
 */
public class MarshallingSupportTest extends TestCase {

    /**
     * @throws java.lang.Exception
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
    }

    /**
     * @throws java.lang.Exception
     * @see junit.framework.TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Test method for
     * {@link org.apache.activemq.util.MarshallingSupport#propertiesToString(java.util.Properties)}.
     * 
     * @throws Exception
     */
    public void testPropertiesToString() throws Exception {
        Properties props = new Properties();
        for (int i = 0; i < 10; i++) {
            String key = "key" + i;
            String value = "value" + i;
            props.put(key, value);
        }
        String str = MarshallingSupport.propertiesToString(props);
        Properties props2 = MarshallingSupport.stringToProperties(str);
        assertEquals(props, props2);
    }
}
