---
title: "JAXB 2.0 API is being loaded from the bootstrap classloader, but this RI (xxx) needs 2.1 API"
slug: /jaxb-20-api-is-being-loaded-from-the-bootstrap-classloader-but-this-ri-xxx-needs-21-api
---

When using JDK 6 (up to JDK 6 Update 3) to run ActiveMQ you can run into this exception. There are two solutions to this:

*   Upgrade to JDK to 1.6.0_04 or above, which includes JAXB 2.1
*   Copy JAXB 2.1 to <JAVA_HOME>/lib/endorsed to override the API jars that ship with the JDK

