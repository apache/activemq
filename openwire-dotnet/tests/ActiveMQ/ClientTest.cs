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
using System;
using System.IO;

using NUnit.Framework;

using OpenWire.Client;
using OpenWire.Client.Core;

namespace OpenWire.Client
{
    [ TestFixture ]
    public class ClientTest : TestSupport
    {
        string expected = "Hello World!";
        
        [ Test ]
        public override void SendAndSyncReceive()
        {
            base.SendAndSyncReceive();
        }
        
        protected override IMessage CreateMessage(ISession session)
        {
            IMessage request = session.CreateTextMessage(expected);
            return request;
        }
        
        protected override void AssertValidMessage(IMessage message)
        {
            ITextMessage textMessage = (ITextMessage) message;
            String text = textMessage.Text;
            Console.WriteLine("Received message with text: " + text);
            Assert.AreEqual(expected, text, "the message text");
        }
        
    }
}

