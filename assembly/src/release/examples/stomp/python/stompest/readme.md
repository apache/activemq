Prereqs
=======

Install the [stomppy](http://code.google.com/p/stomppy) python client
library.

easy_install users can install it by running:

     easy_install stompest

If you use `pip`:
			
     pip install stompest

The stompest client library supports a blocking API, and you can find an
example of it's use in the `sync` directory.  It also supports using 
a non-blocking API based on Twisted, and that example can be found in 
the `async` directory.

To run the `async` examples install `stompest.async`:

     easy_install stompest.async

If you use `pip`:

     pip install stompest.async