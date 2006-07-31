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

#include <errno.h>
#include <stdio.h>

#include "Lock.h"
#include "Exception.h"
#include "RCSID.h"

using ActiveMQ::Lock;
using ActiveMQ::Exception;

RCSID(Lock, "$Id$");

Lock::Lock(pthread_mutex_t *m)
  : mutex_(m)
{
    if (NULL == m)
        throw Exception("NULL mutex passed to Lock");
    int rc = pthread_mutex_lock(m);
    if (rc != 0)
        throw Exception(errno >= sys_nerr
                        ? "Unknown error"
                        : sys_errlist[errno]);
    locked_ = true;
}

void
Lock::unlock() {
    if (locked_) {
        pthread_mutex_unlock(mutex_);
        locked_ = false;
    }
}

Lock::~Lock() {
    unlock();
}
