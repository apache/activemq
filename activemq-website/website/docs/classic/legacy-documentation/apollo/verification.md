---
title: "Title"
slug: /verification
---

       

Verification
============

You can use the ruby examples included in the distribution to verify that the broker is operating correctly.

If you have not already done so, install the `stomp` Ruby gem.

    gem install stomp

Change to the `examples/stomp/ruby` directory that was included in the Apollo distribution. Then in a terminal window, run:

### Unix/Linux/OS X

    cd `${APOLLO_HOME}`/examples/stomp/ruby
    ruby listener.rb

### Windows

    cd %APOLLO_HOME%\examples\stomp\ruby
    ruby listener.rb
    

  

Then in a separate terminal window, run:

### Unix/Linux/OS X

    cd `${APOLLO_HOME}`/examples/stomp/ruby
    ruby publisher.rb

### Windows

    cd %APOLLO_HOME%\examples\stomp\ruby
    ruby publisher.rb
    

  

If everything is working well, the publisher should produce output similar to:

    Sent 1000 messages
    Sent 1000 messages
    ...

The consumer's output should look like:

    Received 1000 messages.
    Received 2000 messages.
    ...