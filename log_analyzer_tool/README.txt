Readme file for the LogAnalyzer application.

1. Requirements:
-Python 2.5.1 (http://www.python.org/download/releases/2.5.1/ or your favorite package)
-wxPython 2.8.4 (http://www.wxpython.org/download.php or your favorite package)

2. How to execute:
Run 'python Main.py'.

3. Some instructions:
-This tool will analyze ActiveMQ log files that have been produced
using the 'custom' transport log format. To analyze the files,
put them in a directory, choose that directory and click 'Parse'.
Please don't put any other kind of files in the same directory
(sub-directories won't cause any problem, but the files inside
them will not be analyzed).
For example, imagine you have a setup with 4 machines: 1 has producers,
2 are brokers, and 1 has consumers. As long as you have 1 JVM per machine,
you should have 4 log files. Call the files p.log, b1.log, b2.log,
and c.log, for example. Put the 4 files in the same directory,
choose that directory and click the 'Parse' button.

-The first tab of the tool shows incorrect situations at transport level:
(i) Messages that were sent through a connection, but were not received
at the other end.
(ii) Messages that were received through a connection, but were not sent
(probably you are missing the log file of the JVM that sent the message).
(iii) Messages that are sent 2 times through the same connection.
(iv) Messages that were sent 2 times by the same JVM, but through
different connections.
By clicking the 'Show results with short ids' checkbox, you can switch
between the real connection / producer id used by ActiveMQ,
or a unique integer assigned by the tool.
Often it's easier to compare and browse with this integers than with
the original id's which are often long strings.
The 'Message id' column shows 2 things: the id of the producer that
originally issued the message, and the 'Producer Sequence Id' of a message.
These 2 items identify a message in a unique way.

You can use the checkboxes to filter per type.
You can also filter by a given connection (but then problems of type (iv)
will not appear because they 'belong' to more than one connection).
You can input a 'long id' (the original ActiveMQ id) or a 'short id'
(a short integer assigned by the tool to each connection).

-The second tab of the tool allows you to get a lot of information
about a single message. Input the producer id of the original producer
of the message, and the message's 'Producer Sequence Id'.
You can choose to use the original ActiveMQ producer id (long id)
or the short integer assigned to a producer by the tool.
You can also use the 'Jump to Message Browsing' button of the 1st tab
to see the information about the problems of a given message
without having to copy the message id manually.
In this tab you can also use the 'Show results with short ids' checkbox.

-The third tab gives a summary of the clients (producer and consumers)
which appear in the log files. Each client is identified by a short id,
and belongs to a connection (whose 'short id' and 'long id' are shown).

-The fourth tab gives a summary of the connections involved,
and the clients that belong to them.

-The fifth tab gives a summary of the log files analyzed,
and the connections in each of the files.