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

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.MessageEvaluationContext;

import junit.framework.TestCase;

/**
 *
 */
public class SelectorTest extends TestCase {

    public void testBooleanSelector() throws Exception {
        Message message = createMessage();

        assertSelector(message, "(trueProp OR falseProp) AND trueProp", true);
        assertSelector(message, "(trueProp OR falseProp) AND falseProp", false);
        assertSelector(message, "trueProp", true);

    }

    public void testXPathSelectors() throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();

        message.setJMSType("xml");
        message.setText("<root><a key='first' num='1'/><b key='second' num='2'>b</b></root>");

        assertSelector(message, "XPATH 'root/a'", true);
        assertSelector(message, "XPATH '//root/b'", true);
        assertSelector(message, "XPATH 'root/c'", false);
        assertSelector(message, "XPATH '//root/b/text()=\"b\"'", true);
        assertSelector(message, "XPATH '//root/b=\"b\"'", true);
        assertSelector(message, "XPATH '//root/b=\"c\"'", false);
        assertSelector(message, "XPATH '//root/b!=\"c\"'", true);

        assertSelector(message, "XPATH '//root/*[@key=''second'']'", true);
        assertSelector(message, "XPATH '//root/*[@key=''third'']'", false);
        assertSelector(message, "XPATH '//root/a[@key=''first'']'", true);
        assertSelector(message, "XPATH '//root/a[@num=1]'", true);
        assertSelector(message, "XPATH '//root/a[@key=''second'']'", false);

        assertSelector(message, "XPATH '/root/*[@key=''first'' or @key=''third'']'", true);
        assertSelector(message, "XPATH '//root/*[@key=''third'' or @key=''forth'']'", false);

        assertSelector(message, "XPATH '/root/b=''b'' and /root/b[@key=''second'']'", true);
        assertSelector(message, "XPATH '/root/b=''b'' and /root/b[@key=''first'']'", false);

        assertSelector(message, "XPATH 'not(//root/a)'", false);
        assertSelector(message, "XPATH 'not(//root/c)'", true);
        assertSelector(message, "XPATH '//root/a[not(@key=''first'')]'", false);
        assertSelector(message, "XPATH '//root/a[not(not(@key=''first''))]'", true);

        assertSelector(message, "XPATH 'string(//root/b)'", true);
        assertSelector(message, "XPATH 'string(//root/a)'", false);

        assertSelector(message, "XPATH 'sum(//@num) < 10'", true);
        assertSelector(message, "XPATH 'sum(//@num) > 10'", false);

        assertSelector(message, "XPATH '//root/a[@num > 1]'", false);
        assertSelector(message, "XPATH '//root/b[@num > 1]'", true);

    }

    public void testJMSPropertySelectors() throws Exception {
        Message message = createMessage();
        message.setJMSType("selector-test");
        message.setJMSMessageID("id:test:1:1:1:1");

        assertSelector(message, "JMSType = 'selector-test'", true);
        assertSelector(message, "JMSType = 'crap'", false);

        assertSelector(message, "JMSMessageID = 'id:test:1:1:1:1'", true);
        assertSelector(message, "JMSMessageID = 'id:not-test:1:1:1:1'", false);

        message = createMessage();
        message.setJMSType("1001");

        assertSelector(message, "JMSType='1001'", true);
        assertSelector(message, "JMSType='1001' OR JMSType='1002'", true);
        assertSelector(message, "JMSType = 'crap'", false);
    }

    public void testBasicSelectors() throws Exception {
        Message message = createMessage();

        assertSelector(message, "name = 'James'", true);
        assertSelector(message, "rank > 100", true);
        assertSelector(message, "rank >= 123", true);
        assertSelector(message, "rank >= 124", false);

    }

    public void testPropertyTypes() throws Exception {
        Message message = createMessage();
        assertSelector(message, "byteProp = 123", true);
        assertSelector(message, "byteProp = 10", false);
        assertSelector(message, "byteProp2 = 33", true);
        assertSelector(message, "byteProp2 = 10", false);

        assertSelector(message, "shortProp = 123", true);
        assertSelector(message, "shortProp = 10", false);

        assertSelector(message, "shortProp = 123", true);
        assertSelector(message, "shortProp = 10", false);

        assertSelector(message, "intProp = 123", true);
        assertSelector(message, "intProp = 10", false);

        assertSelector(message, "longProp = 123", true);
        assertSelector(message, "longProp = 10", false);

        assertSelector(message, "floatProp = 123", true);
        assertSelector(message, "floatProp = 10", false);

        assertSelector(message, "doubleProp = 123", true);
        assertSelector(message, "doubleProp = 10", false);
    }

    public void testAndSelectors() throws Exception {
        Message message = createMessage();

        assertSelector(message, "name = 'James' and rank < 200", true);
        assertSelector(message, "name = 'James' and rank > 200", false);
        assertSelector(message, "name = 'Foo' and rank < 200", false);
        assertSelector(message, "unknown = 'Foo' and anotherUnknown < 200", false);
    }

    public void testOrSelectors() throws Exception {
        Message message = createMessage();

        assertSelector(message, "name = 'James' or rank < 200", true);
        assertSelector(message, "name = 'James' or rank > 200", true);
        assertSelector(message, "name = 'Foo' or rank < 200", true);
        assertSelector(message, "name = 'Foo' or rank > 200", false);
        assertSelector(message, "unknown = 'Foo' or anotherUnknown < 200", false);
    }

    public void testPlus() throws Exception {
        Message message = createMessage();

        assertSelector(message, "rank + 2 = 125", true);
        assertSelector(message, "(rank + 2) = 125", true);
        assertSelector(message, "125 = (rank + 2)", true);
        assertSelector(message, "rank + version = 125", true);
        assertSelector(message, "rank + 2 < 124", false);
        assertSelector(message, "name + '!' = 'James!'", true);
    }

    public void testMinus() throws Exception {
        Message message = createMessage();

        assertSelector(message, "rank - 2 = 121", true);
        assertSelector(message, "rank - version = 121", true);
        assertSelector(message, "rank - 2 > 122", false);
    }

    public void testMultiply() throws Exception {
        Message message = createMessage();

        assertSelector(message, "rank * 2 = 246", true);
        assertSelector(message, "rank * version = 246", true);
        assertSelector(message, "rank * 2 < 130", false);
    }

    public void testDivide() throws Exception {
        Message message = createMessage();

        assertSelector(message, "rank / version = 61.5", true);
        assertSelector(message, "rank / 3 > 100.0", false);
        assertSelector(message, "rank / 3 > 100", false);
        assertSelector(message, "version / 2 = 1", true);

    }

    public void testBetween() throws Exception {
        Message message = createMessage();

        assertSelector(message, "rank between 100 and 150", true);
        assertSelector(message, "rank between 10 and 120", false);
    }

    public void testIn() throws Exception {
        Message message = createMessage();

        assertSelector(message, "name in ('James', 'Bob', 'Gromit')", true);
        assertSelector(message, "name in ('Bob', 'James', 'Gromit')", true);
        assertSelector(message, "name in ('Gromit', 'Bob', 'James')", true);

        assertSelector(message, "name in ('Gromit', 'Bob', 'Cheddar')", false);
        assertSelector(message, "name not in ('Gromit', 'Bob', 'Cheddar')", true);
    }

    public void testIsNull() throws Exception {
        Message message = createMessage();

        assertSelector(message, "dummy is null", true);
        assertSelector(message, "dummy is not null", false);
        assertSelector(message, "name is not null", true);
        assertSelector(message, "name is null", false);
    }

    public void testLike() throws Exception {
        Message message = createMessage();
        message.setStringProperty("modelClassId", "com.whatever.something.foo.bar");
        message.setStringProperty("modelInstanceId", "170");
        message.setStringProperty("modelRequestError", "abc");
        message.setStringProperty("modelCorrelatedClientId", "whatever");

        assertSelector(message, "modelClassId LIKE 'com.whatever.something.%' AND modelInstanceId = '170' AND (modelRequestError IS NULL OR modelCorrelatedClientId = 'whatever')",
                       true);

        message.setStringProperty("modelCorrelatedClientId", "shouldFailNow");

        assertSelector(message, "modelClassId LIKE 'com.whatever.something.%' AND modelInstanceId = '170' AND (modelRequestError IS NULL OR modelCorrelatedClientId = 'whatever')",
                       false);

        message = createMessage();
        message.setStringProperty("modelClassId", "com.whatever.something.foo.bar");
        message.setStringProperty("modelInstanceId", "170");
        message.setStringProperty("modelCorrelatedClientId", "shouldNotMatch");

        assertSelector(message, "modelClassId LIKE 'com.whatever.something.%' AND modelInstanceId = '170' AND (modelRequestError IS NULL OR modelCorrelatedClientId = 'whatever')",
                       true);
    }

    /**
     * Test cases from Mats Henricson
     */
    public void testMatsHenricsonUseCases() throws Exception {
        Message message = createMessage();
        assertSelector(message, "SessionserverId=1870414179", false);

        message.setLongProperty("SessionserverId", 1870414179);
        assertSelector(message, "SessionserverId=1870414179", true);

        message.setLongProperty("SessionserverId", 1234);
        assertSelector(message, "SessionserverId=1870414179", false);

        assertSelector(message, "Command NOT IN ('MirrorLobbyRequest', 'MirrorLobbyReply')", false);

        message.setStringProperty("Command", "Cheese");
        assertSelector(message, "Command NOT IN ('MirrorLobbyRequest', 'MirrorLobbyReply')", true);

        message.setStringProperty("Command", "MirrorLobbyRequest");
        assertSelector(message, "Command NOT IN ('MirrorLobbyRequest', 'MirrorLobbyReply')", false);
        message.setStringProperty("Command", "MirrorLobbyReply");
        assertSelector(message, "Command NOT IN ('MirrorLobbyRequest', 'MirrorLobbyReply')", false);
    }

    public void testFloatComparisons() throws Exception {
        Message message = createMessage();

        assertSelector(message, "1.0 < 1.1", true);
        assertSelector(message, "-1.1 < 1.0", true);
        assertSelector(message, "1.0E1 < 1.1E1", true);
        assertSelector(message, "-1.1E1 < 1.0E1", true);

        assertSelector(message, "1. < 1.1", true);
        assertSelector(message, "-1.1 < 1.", true);
        assertSelector(message, "1.E1 < 1.1E1", true);
        assertSelector(message, "-1.1E1 < 1.E1", true);

        assertSelector(message, ".1 < .5", true);
        assertSelector(message, "-.5 < .1", true);
        assertSelector(message, ".1E1 < .5E1", true);
        assertSelector(message, "-.5E1 < .1E1", true);

        assertSelector(message, "4E10 < 5E10", true);
        assertSelector(message, "5E8 < 5E10", true);
        assertSelector(message, "-4E10 < 2E10", true);
        assertSelector(message, "-5E8 < 2E2", true);
        assertSelector(message, "4E+10 < 5E+10", true);
        assertSelector(message, "4E-10 < 5E-10", true);
    }

    public void testStringQuoteParsing() throws Exception {
        Message message = createMessage();
        assertSelector(message, "quote = '''In God We Trust'''", true);
    }

    public void testLikeComparisons() throws Exception {
        Message message = createMessage();

        assertSelector(message, "quote LIKE '''In G_d We Trust'''", true);
        assertSelector(message, "quote LIKE '''In Gd_ We Trust'''", false);
        assertSelector(message, "quote NOT LIKE '''In G_d We Trust'''", false);
        assertSelector(message, "quote NOT LIKE '''In Gd_ We Trust'''", true);

        assertSelector(message, "foo LIKE '%oo'", true);
        assertSelector(message, "foo LIKE '%ar'", false);
        assertSelector(message, "foo NOT LIKE '%oo'", false);
        assertSelector(message, "foo NOT LIKE '%ar'", true);

        assertSelector(message, "foo LIKE '!_%' ESCAPE '!'", true);
        assertSelector(message, "quote LIKE '!_%' ESCAPE '!'", false);
        assertSelector(message, "foo NOT LIKE '!_%' ESCAPE '!'", false);
        assertSelector(message, "quote NOT LIKE '!_%' ESCAPE '!'", true);

        assertSelector(message, "punctuation LIKE '!#$&()*+,-./:;<=>?@[\\]^`{|}~'", true);
    }

    public void testSpecialEscapeLiteral() throws Exception {
        Message message = createMessage();
        assertSelector(message, "foo LIKE '%_%' ESCAPE '%'", true);
        assertSelector(message, "endingUnderScore LIKE '_D7xlJIQn$_' ESCAPE '$'", true);
        assertSelector(message, "endingUnderScore LIKE '_D7xlJIQn__' ESCAPE '_'", true);
        assertSelector(message, "endingUnderScore LIKE '%D7xlJIQn%_' ESCAPE '%'", true);
        assertSelector(message, "endingUnderScore LIKE '%D7xlJIQn%'  ESCAPE '%'", true);

        // literal '%' at the end, no match
        assertSelector(message, "endingUnderScore LIKE '%D7xlJIQn%%'  ESCAPE '%'", false);

        assertSelector(message, "endingUnderScore LIKE '_D7xlJIQn\\_' ESCAPE '\\'", true);
        assertSelector(message, "endingUnderScore LIKE '%D7xlJIQn\\_' ESCAPE '\\'", true);
    }

    public void testInvalidSelector() throws Exception {
        Message message = createMessage();
        assertInvalidSelector(message, "3+5");
        assertInvalidSelector(message, "True AND 3+5");
        assertInvalidSelector(message, "=TEST 'test'");
        assertInvalidSelector(message, "prop1 = prop2 foo AND string = 'Test'");
        assertInvalidSelector(message, "a = 1 AMD  b = 2");
    }

    public void testFunctionSelector() throws Exception {
        Message message = createMessage();
        assertSelector(message, "REGEX('1870414179', SessionserverId)", false);
        message.setLongProperty("SessionserverId", 1870414179);
        assertSelector(message, "REGEX('1870414179', SessionserverId)", true);
        assertSelector(message, "REGEX('[0-9]*', SessionserverId)", true);
        assertSelector(message, "REGEX('^[1-8]*$', SessionserverId)", false);
        assertSelector(message, "REGEX('^[1-8]*$', SessionserverId)", false);

        assertSelector(message, "INLIST(SPLIT('Tom,Dick,George',','), name)", false);
        assertSelector(message, "INLIST(SPLIT('Tom,James,George',','), name)", true);

        assertSelector(message, "INLIST(MAKELIST('Tom','Dick','George'), name)", false);
        assertSelector(message, "INLIST(MAKELIST('Tom','James','George'), name)", true);

        assertSelector(message, "REGEX('connection1111', REPLACE(JMSMessageID,':',''))", true);
    }

    protected Message createMessage() throws JMSException {
        Message message = createMessage("FOO.BAR");
        message.setJMSType("selector-test");
        message.setJMSMessageID("connection:1:1:1:1");
        message.setObjectProperty("name", "James");
        message.setObjectProperty("location", "London");

        message.setByteProperty("byteProp", (byte)123);
        message.setByteProperty("byteProp2", (byte)33);
        message.setShortProperty("shortProp", (short)123);
        message.setIntProperty("intProp", 123);
        message.setLongProperty("longProp", 123);
        message.setFloatProperty("floatProp", 123);
        message.setDoubleProperty("doubleProp", 123);

        message.setIntProperty("rank", 123);
        message.setIntProperty("version", 2);
        message.setStringProperty("quote", "'In God We Trust'");
        message.setStringProperty("foo", "_foo");
        message.setStringProperty("punctuation", "!#$&()*+,-./:;<=>?@[\\]^`{|}~");
        message.setStringProperty("endingUnderScore", "XD7xlJIQn_");

        message.setBooleanProperty("trueProp", true);
        message.setBooleanProperty("falseProp", false);
        return message;
    }

    protected void assertInvalidSelector(Message message, String text) throws JMSException {
        try {
            SelectorParser.parse(text);
            fail("Created a valid selector");
        } catch (InvalidSelectorException e) {
        }
    }

    protected void assertSelector(Message message, String text, boolean expected) throws JMSException {
        BooleanExpression selector = SelectorParser.parse(text);
        assertTrue("Created a valid selector", selector != null);
        MessageEvaluationContext context = new MessageEvaluationContext();
        context.setMessageReference((org.apache.activemq.command.Message)message);
        boolean value = selector.matches(context);
        assertEquals("Selector for: " + text, expected, value);
    }

    protected Message createMessage(String subject) throws JMSException {
        ActiveMQMessage message = new ActiveMQMessage();
        message.setJMSDestination(new ActiveMQTopic(subject));
        return message;
    }
}
