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

#ifndef ACTIVEMQ_SEMAPHORE_H
#define ACTIVEMQ_SEMAPHORE_H

#include <semaphore.h>
#include <string>

namespace ActiveMQ {

    /// Abstraction class for Semaphores
    /**
       This class holds a logical semaphore.  Since semaphores don't
       have exactly the same API across platforms, this class is
       provided to keep preprocessor statements out of the rest of the
       code.
    */
    class Semaphore {
    public:
        /// Constructs a new semaphore
        explicit Semaphore(unsigned int defvalue = 0);

        /// Posts on the semaphore, waking a blocked thread
        void post();

        /// Waits for another thread to post
        void wait();

        ~Semaphore();
    private:
        sem_t *sem_;

        static std::string getName_();
        static int counter_;
    };
};

#endif // ACTIVEMQ_SEMAPHORE_H
