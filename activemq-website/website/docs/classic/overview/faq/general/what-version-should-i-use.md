---
title: "What version should I use"
slug: /what-version-should-i-use
---

ActiveMQ version numbers follow the **MAJOR.MINOR.PATCH** convention used by many software projects.  In general, patch releases are done only when there are significant enough bugs found in the current minor version to justify a release sooner than the next minor release (though 5.9.1 was an exception, as it was just as large as a minor release).  As a result, you should generally use the most recent patch version of whichever minor release you choose; there is very seldom a reason to pick a patch version other than the latest within a given minor version.

You should also generally use the most recent minor version if possible since it will include bug fixes that may let you avoid being caught by problems in the older minor versions, though as always you may have reasons to deviate from this general rule (bugs introduced in the newer minor version, difficulty getting your company's security department to approve a new version, etc.).

In all cases, when deciding what version to use you should review the release notes of the later version and of all intermediate versions (e.g. if deciding between 5.9.1 and 5.11.1, you should review the release notes for 5.10.0, 5.11.0, and 5.11.1) to understand what bugs are fixed in the later version, and you should review the release notes for versions after the later version as well as the unresolved issues in [JIRA](http://thoughts and suggestions) to understand what bugs were introduced after the earlier version.

