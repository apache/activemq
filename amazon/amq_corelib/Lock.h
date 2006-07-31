/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
  
  http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
*/

#ifndef ACTIVEMQ_LOCK_H
#define ACTIVEMQ_LOCK_H

#include <pthread.h>

namespace ActiveMQ {
    /// RAII lock class, used internally
    /**
       This class holds a lock for its lifetime.  The constructor
       acquires the lock, and the destructor releases it.  This is to
       allow exception-safe locking (the lock will be released during
       stack unwind).

       @version $Id$
    */
    class Lock {
    public:
        Lock(pthread_mutex_t *m);
        /// Explicit unlock for where implicit unlocking is inconvenient.
        void unlock();
        ~Lock();
    private:
        pthread_mutex_t *mutex_;
        bool locked_;
    };
};

#endif // ACTIVEMQ_LOCK_H
