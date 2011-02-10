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
package org.apache.activemq.selector;

import junit.framework.TestCase;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.ComparisonExpression;
import org.apache.activemq.filter.Expression;
import org.apache.activemq.filter.LogicExpression;
import org.apache.activemq.filter.PropertyExpression;
import org.apache.activemq.filter.XPathExpression;
import org.apache.activemq.spring.ConsumerBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version $Revision: 1.2 $
 */
public class SelectorParserTest extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(SelectorParserTest.class);

    public void testParseXPath() throws Exception {
        BooleanExpression filter = parse("XPATH '//title[@lang=''eng'']'");
        assertTrue("Created XPath expression", filter instanceof XPathExpression);
        LOG.info("Expression: " + filter);
    }

    public void testParseWithParensAround() throws Exception {
        String[] values = {"x = 1 and y = 2", "(x = 1) and (y = 2)", "((x = 1) and (y = 2))"};

        for (int i = 0; i < values.length; i++) {
            String value = values[i];
            LOG.info("Parsing: " + value);

            BooleanExpression andExpression = parse(value);
            assertTrue("Created LogicExpression expression", andExpression instanceof LogicExpression);
            LogicExpression logicExpression = (LogicExpression)andExpression;
            Expression left = logicExpression.getLeft();
            Expression right = logicExpression.getRight();

            assertTrue("Left is a binary filter", left instanceof ComparisonExpression);
            assertTrue("Right is a binary filter", right instanceof ComparisonExpression);
            ComparisonExpression leftCompare = (ComparisonExpression)left;
            ComparisonExpression rightCompare = (ComparisonExpression)right;
            assertPropertyExpression("left", leftCompare.getLeft(), "x");
            assertPropertyExpression("right", rightCompare.getLeft(), "y");
        }
    }

    protected void assertPropertyExpression(String message, Expression expression, String expected) {
        assertTrue(message + ". Must be PropertyExpression", expression instanceof PropertyExpression);
        PropertyExpression propExp = (PropertyExpression)expression;
        assertEquals(message + ". Property name", expected, propExp.getName());
    }

    protected BooleanExpression parse(String text) throws Exception {
        return SelectorParser.parse(text);
    }
}
