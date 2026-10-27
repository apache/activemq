---
title: "Release Guide"
slug: /release-guide
---


How to create and announce an ActiveMQ release. This release is based on [General guide for releasing Maven-based project at Apache](http://maven.apache.org/developers/release/apache-release.html) , so be sure to check it out before continuing and meet all prerequisites.

Maven Setup
-------------

Before you deploy anything to the maven repository using Maven, you should configure your ~/.m2/settings.xml file  
so that the file permissions of the deployed artifacts are group writeable. If you do not do this, other developers will not able to overwrite your SNAPSHOT releases with newer versions.

```
<settings>
  ...
  <servers>

    <server>
      <id>apache.snapshots.https</id>
      <username>user</username>
      <password>password</password>
    </server>
    <server>
      <id>apache.releases.https</id>
      <username>user</username>
      <password>password</password>
    </server>

  </servers>
  ...
</settings>
```

### Additional local configuration for using release and staging plugins.

To effectively use the release and staging plugins you need some information about where the staging will happen and signing information for gpg. Your ~/.m2/settings.xml should contain a profile like this:

```
<settings>
    <profiles>
        <profile>
          <id>apache-release</id>
          <properties>
             <gpg.passphrase>secretPhrase</gpg.passphrase>
         </properties>
        </profile>
    </profiles>
 ...
</settings>
```

Creating the ActiveMQ Release
-----------------------------

If the release is the first one on a new series, you might want to create a branch for the series:

```
git branch activemq-a.b.x main
git push origin activemq-a.b.x
```

1. As ActiveMQ protects branches, you have to create a release branch based on the branch you want to release:

```
git branch release/a.b.c activemq-a.b.x
git push origin release/a.b.c
```

2. [Optional] You can do a verification build on the `activemq-a.b.c` branch:

```
mvn verify
```

Especially, it should not complain about missing license header (rat) and the build should pass without error.

3. You can now go on the release branch and prepare the release:

```
git checkout release/a.b.c
mvn release:prepare -Papache-release,deploy
```

This create the RC tag.

4. Now we can stage the release:

```
mvn release:perform -Papache-release,deploy
```

It creates a staging repository on Nexus (https://repository.apache.org).

5. You can close the staging repository on Nexus. Go on https://repository.apache.org, in the Staging Repositories menu and close the ActiveMQ staging repository.

6. Now, you can stage the release artifacts on Dist:

```
svn co https://dist.apache.org/repos/dist/dev/activemq/activemq
cd activemq
./prepare-release.sh <nexus-staging-repo-url> <version>
svn add <version>
svn commit
```

7. Create the pre-release on GitHub (https://github.com/apache/activemq/releases/new) using the corresponding tags and using "Apache ActiveMQ a.b.c" as release title.

8. Call a vote on the dev mailing list, with this template:

```
Subject: [VOTE] Apache ActiveMQ 6.2.7 release (rcN)

Hi everyone,

I propose Apache ActiveMQ a.b.c (rcN) release for your vote.

This release includes:
- ...

You can review the Pre-Release Notes for details:
https://github.com/apache/activemq/releases/tag/activemq-a.b.c

Staging Maven Repository:
https://repository.apache.org/content/repositories/orgapacheactivemq-xxxx/

Staging Dist Repository:
https://dist.apache.org/repos/dist/dev/activemq/activemq/a.b.c/

Git tag:
https://github.com/apache/activemq/tree/activemq-a.b.c

Please vote to approve this release:
[ ] +1 Approve the release
[ ] 0 I don't care
[ ] -1 Don't approve the release (please provide specific comment)

This vote will be open for at least 72 hours.

Thanks!
Regards
```

After the vote passes
---------------------

1. Promote the release on Nexus (https://repository.apache.org).

2. Promote the release on Dist:

```
svn mv https://dist.apache.org/repos/dist/dev/activemq/activemq/<version> https://dist.apache.org/repos/dist/release/activemq/
```

3. Change the release on GitHub removing the pre-release tag (https://github.com/apache/activemq/releases/edit/activemq-a.b.c).

4. Update the website with release:

```
git clone https://github.com/apache/activemq-website
cd activemq-website/src/_classic_releases
vi classic-0a-0b-0c.md
git add
git commit -a
git push
```

5. Announce the release on the dev mailing list and announce@apache.org:

```
Subject: [ANNOUNCE] Apache ActiveMQ a.b.c has been released!

The ActiveMQ team is pleased to announce the Apache ActiveMQ a.b.c release.

<Quickly describe the release>

You view find the details on the Release page:
https://activemq.apache.org/components/classic/download/classic-0a-0b-0c

And also more details on the Release Notes:
https://github.com/apache/activemq/releases/tag/activemq-a.b.c

You can download ActiveMQ a.b.c here:
https://activemq.apache.org/components/classic/download/

Enjoy!

Regards
--
The Apache ActiveMQ team
```

6. Create a PR to merge the release branch to bump to the next snapshot version

```
For example, create a PR to merge release/a.b.c into activemq-a.b.x
```
7. After merging the release branch and bumping the version, the release branch can be deleted
