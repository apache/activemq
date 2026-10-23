---
title: "Enable OpenSSL support with autotools"
slug: /enable-openssl-support-with-autotools
---

When you build the ActiveMQ-CPP library using the GNU AutoTools the Configure script will automatically search for OpenSSL on your system and enable support for it in the code if its found. The search might fail if you have installed OpenSSL in a non-standard location in which case you can tell the configure script where to look using the _with-openssl_ option, see below for an example.
```
./configure --with-openssl=/opt/special/openssl
```
