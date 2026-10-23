---
title: "ActiveMQ-CPP product version number"
---

# ActiveMQ-CPP product version number

_This is a DRAFT proposal and has not been adopted as official policy by this project._

In order to facilitate the correct use of libtool and the preparation of binary packages for distribution on Debian, Red Hat, etc, it is beneficial to have a written policy on version numbers.

### Product version number

These numbers represent the release of the product. E.g. 2.1.2

For the ActiveMQ-CPP project, the numbers have the format MAJOR.MINOR.REVISION, and the following meanings:

*   MAJOR: a change in the MAJOR number is necessary for anything that changes binary compatibility (e.g. changing a function prototype or class definition), or when major internal changes occur
*   MINOR: a change in the MINOR number is necessary whenever new functionality is added without breaking binary compatibility (e.g. adding a new class), or when anything other than very minor internal changes occur
*   REVISION: a change in the REVISION number is made for any other type of change that does not justify a change to MINOR or MAJOR

Further to the above, it is envisaged that a user who has a packaging system (such as Debian):

*   may choose to have more than one major version installed on the same system concurrently (e.g. 2.1.2 and 3.4.5)
*   may only have one variation of each major version installed (e.g. it is not intended to have 2.1.2 and 2.3.0 concurrently)

### libtool ABI version number

This number represents an ABI version. It is an integer, e.g., the number 5.

The ABI version number does not need to be the same as the product version number.

An increment in the libtool version number is mandatory when one of the following occurs:

*   any new structs, classes or function prototypes are added
*   the definition of any structs, classes or function prototypes are modified
*   any structs, classes or function prototypes are removed

Note that in the first case above, binary compatibility with existing applications is maintained. In the second and third case, binary compatibility is lost, and the libtool `age' variable must also be changed.

