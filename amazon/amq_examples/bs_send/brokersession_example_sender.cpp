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

#include <unistd.h>

#include <iostream>
#include <string>

#include "amq_brokersession/BrokerSession.h"
#include "amq_corelib/TextMessage.h"
#include "amq_corelib/LogLevel.h"
#include "amq_corelib/Logger.h"

using namespace std;
using namespace ActiveMQ;

class MyLogger : public Logger {
public:
    bool isEnabled(const LogLevel& ll) { return true; }
    void logFatal(const string& msg);
    void logError(const string& msg);
    void logWarning(const string& msg);
    void logInform(const string& msg);
    void logDebug(const string& msg);
};

void
MyLogger::logFatal(const string& msg) {
    cerr << "FATAL " << msg << endl;
}

void
MyLogger::logError(const string& msg) {
    cerr << "ERROR " << msg << endl;
}

void
MyLogger::logWarning(const string& msg) {
    cerr << "WARNING " << msg << endl;
}

void
MyLogger::logInform(const string& msg) {
    cerr << "INFORM " << msg << endl;
}

void
MyLogger::logDebug(const string& msg) {
    cerr << "DEBUG " << msg << endl;
}

void except(const Exception& e) {
    cout << "myexcept" << endl;
    cout << e.what() << endl;
}

int main(int argc, char **argv) {
    if (argc < 3) {
        cerr << "Usage: brokersession_example_sender <broker_uri> <num_to_send>" << endl;
        return 1;
    }
    try {
        BrokerSession bs(argv[1]);
        bs.setLogger(new MyLogger());
        bs.setExceptionCallback(except);
        bs.connect();
        Destination d = bs.createTopic("test");
        for (int i = 0; i < atoi(argv[2]); i++) {
            cout << "Sending message.." << endl;
            bs.publish(d, TextMessage("test message"));
            //sleep(2);
        }
        cout << "Done with loop..." << endl;
    } catch (const std::exception &e) {
        cerr << "Exception: " << e.what() << endl;
        return 1;
    }

    cout << "Exiting..." << endl;

    return 0;
}
