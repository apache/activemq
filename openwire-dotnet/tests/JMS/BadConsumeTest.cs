/*
 * Copyright 2006 The Apache Software Foundation or its licensors, as
 * applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using JMS;
using NUnit.Framework;
using System;


namespace JMS
{
	[TestFixture]
    public class BadConsumeTest : JMSTestSupport
    {
		[SetUp]
        override public void SetUp()
        {
			base.SetUp();
        }
		
        [TearDown]
        override public void TearDown()
        {
			base.TearDown();
        }
		
        [Test]
        public void TestBadConsumeOperationToTestExceptions()
        {
			try
			{
				IMessageConsumer consumer = session.CreateConsumer(null);
				Console.WriteLine("Created consumer: " + consumer);
				Assert.Fail("Should  have thrown an exception!");
			}
			catch (Exception e)
			{
				Console.WriteLine("Caught expected exception: " + e);
				Console.WriteLine("Stack: " + e.StackTrace);
			}
        }
    }
}

