/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.activemq.console.command;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class PurgeCommandTest {

    private final List<String> datum;
    private final String expected;

    /**
     * Produces the data for the test.
     *
     * @return
     */
    @Parameters(name = "{index}: convertToSQL92({0})={1}")
    public static Collection<Object[]> produceTestData() {
        List<Object[]> params = new ArrayList<>();
        // wildcard query enclosed by single quotes must be converted into 
        // SQL92 LIKE-statement
        params.add(toParameterArray(
                "(JMSMessageId LIKE '%:10_')",
                "JMSMessageId='*:10?'")
        );

        // query parameter containing wildcard characters but not enclosed by 
        // single quotes must be taken as literal
        params.add(toParameterArray(
                "(JMSMessageId=*:10?)",
                "JMSMessageId=*:10?")
        );
        params.add(toParameterArray(
                "(JMSMessageId=%:10_)",
                "JMSMessageId=%:10_")
        );

        // query parameter not enclosed by single quotes must be taken as literal
        params.add(toParameterArray(
                "(JMSMessageId=SOME_ID)",
                "JMSMessageId=SOME_ID")
        );

        // query parameter not containing wildcard characters but enclosed by 
        // single quotes must not be converted into a SQL92 LIKE-statement
        params.add(toParameterArray(
                "(JMSMessageId='SOME_ID')",
                "JMSMessageId='SOME_ID'")
        );
        params.add(toParameterArray(
                "(JMSMessageId='%:10_')",
                "JMSMessageId='%:10_'")
        );

        // multiple query parameter must be concatenated by 'AND'
        params.add(toParameterArray(
                "(JMSMessageId LIKE '%:10_') AND (JMSPriority>5)",
                "JMSMessageId='*:10?'", "JMSPriority>5")
        );
        params.add(toParameterArray(
                "(JMSPriority>5) AND (JMSMessageId LIKE '%:10_')",
                "JMSPriority>5", "JMSMessageId='*:10?'")
        );

        // a query which is already in SQL92 syntax should not be altered
        params.add(toParameterArray(
                "((JMSPriority>5) AND (JMSMessageId LIKE '%:10_'))",
                "(JMSPriority>5) AND (JMSMessageId LIKE '%:10_')")
        );
        return params;
    }

    /**
     * Test if the wildcard queries correctly converted into a valid SQL92
     * statement.
     */
    @Test
    public void testConvertToSQL92() {
        System.out.print("testTokens  = " + datum);
        System.out.println("  output = " + expected);
        PurgeCommand pc = new PurgeCommand();
        Assert.assertEquals(expected, pc.convertToSQL92(datum));
    }

    /**
     * Convert the passed parameter into an object array which is used for
     * the unit tests of method <code>convertToSQL92</code>.
     *
     * @param datum the tokens which are passed as list to the method
     * @param expected the expected value returned by the method
     * @return object array with the values used for the unit test
     */
    static Object[] toParameterArray(String expected, String... tokens) {
        return new Object[]{Arrays.asList(tokens), expected};
    }

    public PurgeCommandTest(List<String> datum, String expected) {
        this.datum = datum;
        this.expected = expected;
    }
}
