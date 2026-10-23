---
title: "Multicast - Watch out for IPV6 vs IPV4 support on your operating system or distribution or network"
slug: /multicast-watch-out-for-ipv6-vs-ipv4-support-on-your-operating-system-or-distribution-or-network
---

You may wonder why you are not receiving any message at all during multicast, maybe you have just tried everything, and even on some other OS or distributions and JDK and realize that it works in some place and not on other... you may have tried a small C program to do the same..and it worked well.. you're feeling the pressure on your neck...what could be wrong on YOUR application on THIS OS/distribution...

As a default, on the Linux platform, if you have IPV6 support enabled in your kernel, the Java Runtime (since 1.4 version) will use IPV6 sockets to communicate. That's perfectly fine, except that your network may not be configured for IPV6. So everything will be dropped...and nothing will ever be received on the consumer side (which maybe even does not support IPV6).

There's a solution to this problem: Force IPV4 stack to be used by the Java Runtime. This can be done using a system property:
```
 -Djava.net.preferIPv4Stack=true
```

Networking IPv6 User Guide

This information and much more details can be found in the Java Documentation [Networking IPv6 User Guide](http://java.sun.com/j2se/1.5.0/docs/guide/net/ipv6_guide/) and in [Networking Properties](http://java.sun.com/j2se/1.5.0/docs/guide/net/properties.html)

Alternatively you can disable IPV6 support on your Linux machine by adding the following line **at the end of /etc/modprobe.conf** (_as its name suggests immediately, net-pf-10 is the IPV6 networking module_)
```
alias net-pf-10 off # disable IPV6
```
If you have very knowledgeable sysadmins that are used to these problems (like in a TV or Radio company for instance where they use multicast a lot), they will be able to tell you immediately if IPV6 is supported on the network (alternatively they will be able to tell you as well if multicast is supported and on which address/port you can multicast), this is a plus and will help you tremendously.

In some more frequent cases, your sysadmin will be as lost as you are as they are not dealing with these issues very frequently. So good luck for finding out problems. You will need to have a very strict approach to eliminate uncertainties. For this you will need at least a tool like [ethereal](http://www.ethereal.com) (works on unix, linux and windows)

