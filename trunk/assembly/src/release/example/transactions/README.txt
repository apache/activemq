Transactions Demo
=================
This example is an ActiveMQ implementation of the "TransactedExample" from
Sun's JMS Tutorial (http://java.sun.com/products/jms/tutorial/index.html).

The example simulates a simplified eCommerce application with four parts:
the retailer who places the orders, the vendor who assemples the computers,
and two suppliers--one for hard drives and another for monitors.

The retailer sends a message to the vendor's queue and awaits a reply.
The vendor receives the message and sends a message to each of the
supplier's queues. It does this in a single transaction, and will randomly
throw an exception simulating a database error, triggering a rollback.
Each supplier receives the order, checks inventory and replies to the
message stating how many items were sent.
The vendor collects both responses and responds to the retailer, notifying
wheather it cna fulfill the complete order or not.
The retailer receives the message from the vendor.

Running the Example
===================
To run the complete demo in a single JVM, with ActiveMQ running on the local
computer:
  ant transactions_demo

If you are running ActiveMQ on a non-standard port, or on a different host,
you can pass a url on the commandline:
  ant -Durl=tcp://localhost:61616 transactions_demo

If your ActiveMQ instance is password-protected, you can also pass a
username and password on the command line:
  ant -Duser=myusername -Dpassword=supersecret transactions_demo

You can also run the individual components seperately, again with optional
url and/or authentication parameters:
  ant retailer &
  ant vendor &
  ant hdsupplier &
  ant monitorsupplier &

