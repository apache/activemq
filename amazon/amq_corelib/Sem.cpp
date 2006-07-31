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

#include <sys/types.h>
#include <unistd.h>

#include <sstream>

#include "Sem.h"

using ActiveMQ::Semaphore;
using std::string;
using std::stringstream;

Semaphore::Semaphore(unsigned int defvalue) {
#ifdef __APPLE__
    sem_ = sem_open(getName_().c_str(), O_CREAT | O_EXCL, 700, defvalue);
#else
    sem_ = new sem_t();
    sem_init(sem_, 0, defvalue);
#endif
}

void
Semaphore::post() {
    sem_post(sem_);
}

void
Semaphore::wait() {
    sem_wait(sem_);
}

/* static */
string
Semaphore::getName_() {
    stringstream ss;
    ss << getpid() << ":" << ++counter_;
    return ss.str();
}

/* static */
int
Semaphore::counter_ = 0;

Semaphore::~Semaphore() {
#ifdef __APPLE__
    sem_close(sem_);
#else
    sem_destroy(sem_);
    delete sem_;
#endif
}
