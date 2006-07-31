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

#ifndef ACTIVEMQ_EXCEPTIONCALLBACK_H
#define ACTIVEMQ_EXCEPTIONCALLBACK_H

#include <memory>

#include "Exception.h"

namespace ActiveMQ {
    /// Callback for BrokerSession exceptions.
    /**
       This class represents a function or function object that will
       be called for exceptions raised in the BrokerSessions's
       internal event thread.  You shouldn't need to construct it
       yourself - it will be implicitly constructed from any function
       or function object that matches the void (*)(const Exception &)
       signature.

       @version $Id$
    */       
    class ExceptionCallback {
    private:
        struct Impl {
            virtual void operator()(const Exception& msg) const = 0;
            virtual Impl *clone() const = 0;
            virtual ~Impl() {}
        };

        template<class T>
        struct Wrapper : public Impl {
            Wrapper(const T& callfunc) : f_(callfunc) {}
            void operator()(const Exception& msg) const { f_(msg); }
            Impl *clone() const { return new Wrapper<T>(f_); }
            virtual ~Wrapper() {}
        private:
            const T f_;
        };

        std::auto_ptr<Impl> pimpl;

    public:
        ExceptionCallback() : pimpl(NULL) {}
        ExceptionCallback(const ExceptionCallback& oth);

        template<class T>
        ExceptionCallback(T callfunc) : pimpl(new Wrapper<T>(callfunc)) {}

        void operator()(const Exception& msg) const;
    };
};

#endif // ACTIVEMQ_EXCEPTIONCALLBACK_H
