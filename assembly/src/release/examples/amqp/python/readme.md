## Overview
This is an example of how to use the python AMQP [Qpid Proton](https://qpid.apache.org/proton/index.html) reactor API with ActiveMQ.

## Prereqs
- linux
- python 3.5+
- you have successfully installed [python-qpid-proton](https://pypi.python.org/pypi/python-qpid-proton) - including any of its [dependencies](https://github.com/apache/qpid-proton/blob/master/INSTALL.md)
- $PYTHONPATH can search this folder

## Running the Examples
In one terminal window run:

    python sender.py

In another terminal window run:

    python receiver.py

Use the ActiveMQ admin web page to check Messages Enqueued / Dequeued counts match. 

You can control which AMQP server the examples try to connect to and the messages they send by changing the values in config.py
