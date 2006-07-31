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

#ifndef ACTIVEMQ_DESTINATION_H
#define ACTIVEMQ_DESTINATION_H

#include <string>
#include <memory>

#include "command/ActiveMQDestination.h"

namespace ActiveMQ {
    class CoreLib;
    class CoreLibImpl;

    /// Message destination
    /**
       This class holds an ActiveMQ "destination."  It can be either a
       JMS Topic or Queue, or temporary versions of either of those.

       Destinations are always constructed from the BrokerSession or
       CoreLib objects, as there is broker communication involved in
       setting up and tearing down destinations.

       @version $Id$
     */
    class Destination {
    public:
        /// Gets the name of the destination
        const std::string& getName() const { return name_; }

        /// Gets a normalized descriptive string of the destination
        std::string toString() const { return string_; }

        /// indicates whether or not the destination is temporary
        bool isTemporary() const { return temporary_; }

        /// indicates whether or not the destination is a topic
        bool isTopic() const { return isTopic_; }

        Destination();
        virtual ~Destination();
        Destination(const Destination& oth);
        Destination& operator=(const Destination& oth);

	bool operator<(const Destination& oth) const { return name_ < oth.name_; }

        bool operator==(const Destination& oth) const { return name_ == oth.name_ && temporary_ == oth.temporary_ && isTopic_ == oth.isTopic_; }

        bool operator!=(const Destination& oth) const { return !operator==(oth); }
    private:
        std::string name_;
        CoreLib *cl_;
        bool temporary_;
        bool isTopic_;
        std::string string_;

        friend class CoreLibImpl;
        Destination(CoreLib *corelib, const std::string& name, bool isTemp, bool isTopic);
        std::auto_ptr<Command::ActiveMQDestination> createCommandInstance() const;
        void invalidate();
    };
};

#endif // ACTIVEMQ_DESTINATION_H
