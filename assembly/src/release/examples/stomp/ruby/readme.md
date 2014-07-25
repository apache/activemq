Prereqs
=======

- Install RubyGems see: http://docs.rubygems.org/
- Install the stomp gem.  Run: gem install stomp

Overview of stompcat.rb and catstomp.rb 
==========================================

The basic idea behind these scripts to to create something like netcat except over JMS
destinations.

catstomp.rb - takes stdin and sends it to a stomp destination
stompcat.rb - outputs data received from a stomp destination

A simple example usage:

In console 1 run:
cat | ./catstomp.rb

In console 2 run:
./stompcat.rb

now any line you enter into console 1 will get sent to console 2.

Hopefully these to scripts can get merged together in the future and the command line
arguments can change so that it look more like netcat.
