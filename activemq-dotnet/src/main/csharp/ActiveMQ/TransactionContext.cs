/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using ActiveMQ;
using ActiveMQ.Commands;
using System.Collections;


namespace ActiveMQ
{
	public enum TransactionType
    {
        Begin = 0, Prepare = 1, CommitOnePhase = 2, CommitTwoPhase = 3, Rollback = 4, Recover=5, Forget = 6, End = 7
    }
}

namespace ActiveMQ
{
	public class TransactionContext
    {
        private TransactionId transactionId;
        private Session session;
        private ArrayList synchronizations = new ArrayList();
        
        public TransactionContext(Session session)
		{
            this.session = session;
        }
        
        public TransactionId TransactionId
        {
            get { return transactionId; }
        }
        
        /// <summary>
        /// Method AddSynchronization
        /// </summary>
        public void AddSynchronization(ISynchronization synchronization)
        {
            synchronizations.Add(synchronization);
        }
        
        
        public void Begin()
        {
            if (transactionId == null)
            {
                transactionId = session.Connection.CreateLocalTransactionId();
                
                TransactionInfo info = new TransactionInfo();
                info.ConnectionId = session.Connection.ConnectionId;
                info.TransactionId = transactionId;
                info.Type = (int) TransactionType.Begin;
                session.Connection.OneWay(info);
            }
        }
        
        
        public void Rollback()
        {
            if (transactionId != null)
            {
                TransactionInfo info = new TransactionInfo();
                info.ConnectionId = session.Connection.ConnectionId;
                info.TransactionId = transactionId;
                info.Type = (int) TransactionType.Rollback;
                
                transactionId = null;
                session.Connection.OneWay(info);
            }
            
            foreach (ISynchronization synchronization in synchronizations)
			{
                synchronization.AfterRollback();
            }
            synchronizations.Clear();
        }
        
        public void Commit()
        {
            foreach (ISynchronization synchronization in synchronizations)
			{
                synchronization.BeforeCommit();
            }
            
            if (transactionId != null)
            {
                TransactionInfo info = new TransactionInfo();
                info.ConnectionId = session.Connection.ConnectionId;
                info.TransactionId = transactionId;
                info.Type = (int) TransactionType.CommitOnePhase;
                
                transactionId = null;
                session.Connection.OneWay(info);
            }
            
            foreach (ISynchronization synchronization in synchronizations)
			{
                synchronization.AfterCommit();
            }
            synchronizations.Clear();
        }
    }
}

