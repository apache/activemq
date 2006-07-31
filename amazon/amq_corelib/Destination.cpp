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

#include <assert.h>

#include "Destination.h"
#include "CoreLib.h"

#include "command/ActiveMQTempTopic.h"
#include "command/ActiveMQTempQueue.h"
#include "command/ActiveMQQueue.h"
#include "command/ActiveMQTopic.h"

#include <iostream>
#include <sstream>

using namespace ActiveMQ;
using namespace std;

Destination::Destination() :
    name_(""),
    cl_(NULL),
    temporary_(false),
    isTopic_(false)
  {}

Destination::Destination(CoreLib *corelib, const std::string& name, bool isTemp, bool isTopic) :
    name_(name),
    cl_(corelib),
    temporary_(isTemp),
    isTopic_(isTopic) {
    
    stringstream str;
    str << name_;
    str << ":isTemporary=" << temporary_;
    str << ":isTopic=" << isTopic_;
    string_.assign(str.str());

    assert(cl_ != NULL);
    cl_->registerDest(*this);
}

Destination::Destination(const Destination& oth) :
    name_(oth.name_),
    cl_(oth.cl_),
    temporary_(oth.temporary_),
    isTopic_(oth.isTopic_),
    string_(oth.string_)
{
    if (cl_ != NULL) {
        cl_->registerDest(*this);
    }
}

Destination&
Destination::operator=(const Destination& oth) {
    if (&oth != this) {
        if (cl_ != NULL) {
            cl_->unregisterDest(*this);
        }
        cl_ = oth.cl_;
        name_ = oth.name_;
        temporary_ = oth.temporary_;
        isTopic_ = oth.isTopic_;
        string_.assign(oth.string_);
        if (cl_ != NULL) {
            cl_->registerDest(*this);
        }
    }

    return *this;
}

void
Destination::invalidate() {
    cl_ = NULL;
}

Destination::~Destination() {
    if (cl_ != NULL) {
        cl_->unregisterDest(*this);
    }
}

auto_ptr<Command::ActiveMQDestination>
Destination::createCommandInstance() const {
    auto_ptr<Command::ActiveMQDestination> ret;

    if (isTemporary()) {
        if (isTopic())
            ret.reset(new Command::ActiveMQTempTopic());
        else
            ret.reset(new Command::ActiveMQTempQueue());
    }
    else {
        if (isTopic())
            ret.reset(new Command::ActiveMQTopic());
        else
            ret.reset(new Command::ActiveMQQueue());
    }

    ret->setPhysicalName(name_);
    return ret;
}
