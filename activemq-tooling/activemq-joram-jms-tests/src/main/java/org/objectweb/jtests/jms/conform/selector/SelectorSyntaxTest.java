/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.objectweb.jtests.jms.conform.selector;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.objectweb.jtests.jms.framework.PTPTestCase;

/**
 * Test the syntax of of message selector of JMS
 *
 * @author Jeff Mesnil (jmesnil@gmail.com)
 * @version $Id: SelectorSyntaxTest.java,v 1.1 2007/03/29 04:28:35 starksm Exp $
 */
public class SelectorSyntaxTest extends PTPTestCase
{
   /**
    * Test that identifiers that start with a valid Java identifier start character are valid.
    * A valid identifier means that the method <code>Character.isJavaIdentifierStart</code> returns 
    * <code>true</code> for this identifier first character.
    * 
    * @see <a href="http://java.sun.com/j2se/1.3/docs/api/java/lang/Character.html#isJavaIdentifierStart(char)">Character.isJavaIdentifierStart(char)</a>
    */
   public void testValidIdentifiersStart()
   {
      String identifier = null;
      try
      {
         identifier = "_correct";
         Assert.assertTrue(identifier + " starts with an invalid Java identifier start character",
                           Character.isJavaIdentifierStart(identifier.charAt(0)));
         receiver = receiverSession.createReceiver(receiverQueue, identifier + " IS NULL");

         identifier = "$correct";
         Assert.assertTrue(identifier + " starts with an invalid Java identifier start character",
                           Character.isJavaIdentifierStart(identifier.charAt(0)));
         receiver = receiverSession.createReceiver(receiverQueue, identifier + " IS NULL");
      }
      catch (JMSException e)
      {
         Assert.fail(identifier + " is a correct identifier. \n" + e);
      }
   }

   /**
    * Test that identifiers that start with an invalid Java identifier start character are invalid.       
    * 
    * @see #testValidIdentifiersStart()
    */
   public void testInvalidIdentifiersStart()
   {
      String identifier = null;
      try
      {
         identifier = "1uncorrect";

         Assert.assertTrue(identifier + " starts with an invalid Java identifier start character",
                           !Character.isJavaIdentifierStart(identifier.charAt(0)));
         receiver = receiverSession.createReceiver(receiverQueue, identifier + " IS NULL");
         Assert.fail(identifier + " starts with an invalid Java identifier start character");
      }
      catch (JMSException e)
      {
      }

      try
      {
         identifier = "%uncorrect";

         Assert.assertTrue(identifier + " starts with an invalid Java identifier start character",
                           !Character.isJavaIdentifierStart(identifier.charAt(0)));
         receiver = receiverSession.createReceiver(receiverQueue, identifier + " IS NULL");
         Assert.fail(identifier + " starts with an invalid Java identifier start character");
      }
      catch (JMSException e)
      {
      }

   }

   /**
    * Test that message selector can be an empty string.
    */
   public void testEmptyStringAsSelector()
   {
      try
      {
         receiver = receiverSession.createReceiver(receiverQueue, "");
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that identifiers can't be <code>NULL</code>.
    */
   public void testIdentifierNULL()
   {
      try
      {
         receiver = receiverSession.createReceiver(receiverQueue, "NULL = ZERO");
         Assert.fail("NULL is not a valid identifier");
      }
      catch (InvalidSelectorException e)
      {
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that identifiers can't be <code>TRUE</code>.
    */
   public void testIdentifierTRUE()
   {
      try
      {
         receiver = receiverSession.createReceiver(receiverQueue, "TRUE > 0");
         Assert.fail("TRUE is not a valid identifier");
      }
      catch (JMSException e)
      {
      }
   }

   /**
    * Test that identifiers can't be <code>FALSE</code>.
    */
   public void testIdentifierFALSE()
   {
      try
      {
         receiver = receiverSession.createReceiver(receiverQueue, "FALSE > 0");
         Assert.fail("FALSE is not a valid identifier");
      }
      catch (JMSException e)
      {
      }
   }

   /**
    * Test that identifiers can't be <code>NOT</code>.
    */
   public void testIdentifierNOT()
   {
      try
      {
         receiver = receiverSession.createReceiver(receiverQueue, "NOT > 0");
         Assert.fail("NOT is not a valid identifier");
      }
      catch (JMSException e)
      {
      }
   }

   /**
    * Test that identifiers can't be <code>AND</code>.
    */
   public void testIdentifierAND()
   {
      try
      {
         receiver = receiverSession.createReceiver(receiverQueue, "AND > 0");
         Assert.fail("AND is not a valid identifier");
      }
      catch (JMSException e)
      {
      }
   }

   /**
    * Test that identifiers can't be <code>OR</code>.
    */
   public void testIdentifierOR()
   {
      try
      {
         receiver = receiverSession.createReceiver(receiverQueue, "OR > 0");
         Assert.fail("OR is not a valid identifier");
      }
      catch (JMSException e)
      {
      }
   }

   /**
    * Test that identifiers can't be <code>BETWEEN</code>.
    */
   public void testIdentifierBETWEEN()
   {
      try
      {
         receiver = receiverSession.createReceiver(receiverQueue, "BETWEEN > 0");
         Assert.fail("BETWEEN is not a valid identifier");
      }
      catch (JMSException e)
      {
      }
   }

   /**
    * Test that identifiers can't be <code>LIKE</code>.
    */
   public void testIdentifierLIKE()
   {
      try
      {
         receiver = receiverSession.createReceiver(receiverQueue, "LIKE > 0");
         Assert.fail("LIKE is not a valid identifier");
      }
      catch (JMSException e)
      {
      }
   }

   /**
    * Test that identifiers can't be <code>IN</code>.
    */
   public void testIdentifierIN()
   {
      try
      {
         receiver = receiverSession.createReceiver(receiverQueue, "IN > 0");
         Assert.fail("IN is not a valid identifier");
      }
      catch (JMSException e)
      {
      }
   }

   /**
    * Test that identifiers can't be <code>IS</code>.
    */
   public void testIdentifierIS()
   {
      try
      {
         receiver = receiverSession.createReceiver(receiverQueue, "IS > 0");
         Assert.fail("IS is not a valid identifier");
      }
      catch (JMSException e)
      {
      }
   }

   /**
    * Test that identifiers can't be <code>ESCAPE</code>.
    */
   public void testIdentifierESCAPE()
   {
      try
      {
         receiver = receiverSession.createReceiver(receiverQueue, "ESCAPE > 0");
         Assert.fail("ESCAPE is not a valid identifier");
      }
      catch (JMSException e)
      {
      }
   }

   /**
    * Test syntax of "<em>identifier</em> IS [NOT] NULL"
    */
   public void testNull()
   {
      try
      {
         receiver = receiverSession.createReceiver(receiverQueue, "prop_name IS NULL");
         receiver = receiverSession.createReceiver(receiverQueue, "prop_name IS NOT NULL");
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test syntax of "<em>identifier</em> [NOT] LIKE <em>pattern-value</em> [ESCAPE <em>escape-character</em>]"
    */
   public void testLike()
   {
      try
      {
         receiver = receiverSession.createReceiver(receiverQueue, "phone LIKE '12%3'");
         receiver = receiverSession.createReceiver(receiverQueue, "word LIKE 'l_se'");
         receiver = receiverSession.createReceiver(receiverQueue, "underscored LIKE '\\_%' ESCAPE '\\'");
         receiver = receiverSession.createReceiver(receiverQueue, "phone NOT LIKE '12%3'");
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test syntax of "<em>identifier</em> [NOT] IN (<em>string-literal1</em>, <em>string-literal2</em>,...)"
    */
   public void testIn()
   {
      try
      {
         receiver = receiverSession.createReceiver(receiverQueue, "Country IN ('UK', 'US', 'France')");
         receiver = receiverSession.createReceiver(receiverQueue, "Country NOT IN ('UK', 'US', 'France')");
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test syntax of "<em>arithmetic-expr1</em> [NOT] BETWEEN <em>arithmetic-expr2</em> and <em>arithmetic-expr3</em>"
    */
   public void testBetween()
   {
      try
      {
         receiver = receiverSession.createReceiver(receiverQueue, "age BETWEEN 15 and 19");
         receiver = receiverSession.createReceiver(receiverQueue, "age NOT BETWEEN 15 and 19");
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test diffent syntax for approximate numeric literal (+6.2, -95.7, 7.)
    */
   public void testApproximateNumericLiteral()
   {
      try
      {
         receiver = receiverSession.createReceiver(receiverQueue, "average = +6.2");
         receiver = receiverSession.createReceiver(receiverQueue, "average = -95.7");
         receiver = receiverSession.createReceiver(receiverQueue, "average = 7.");
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test diffent syntax for exact numeric literal (+62, -957, 57)
    */
   public void testExactNumericLiteral()
   {
      try
      {
         receiver = receiverSession.createReceiver(receiverQueue, "average = +62");
         receiver = receiverSession.createReceiver(receiverQueue, "max = -957");
         receiver = receiverSession.createReceiver(receiverQueue, "max = 57");
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test diffent syntax for zero as an exact or an approximate numeric literal (0, 0.0, 0.)
    */
   public void testZero()
   {
      try
      {
         receiver = receiverSession.createReceiver(receiverQueue, "max = 0");
         receiver = receiverSession.createReceiver(receiverQueue, "max = 0.0");
         receiver = receiverSession.createReceiver(receiverQueue, "max = 0.");
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test diffent syntax for string literal ('literal' and 'literal''s')
    */
   public void testString()
   {
      try
      {
         receiver = receiverSession.createReceiver(receiverQueue, "string = 'literal'");
         receiver = receiverSession.createReceiver(receiverQueue, "string = 'literal''s'");
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /** 
    * Method to use this class in a Test suite
    */
   public static Test suite()
   {
      return new TestSuite(SelectorSyntaxTest.class);
   }

   public SelectorSyntaxTest(final String name)
   {
      super(name);
   }
}
