---
title: "Creating ActiveMQ-CPP Distributions"
sidebar_position: 3
---
Creating a Distribution of ActiveMQ-CPP
---------------------------------------

This should give you an idea of the steps involved with creating a new distribution of ActiveMQ-CPP. This content was extracted from [http://www.apache.org/dev/mirror-guide-bodewig.html](http://www.apache.org/dev/mirror-guide-bodewig.html), so you may want to reference that for more information.

### A Little Background Info ...

Apache mirrors everything under /www.www.apache.org/dist. We have had the infrastructure team add a directory under there for activemq. All you need in order to write to this directory is to be a member of the activemq group. If you're not, you'll have to create an infrastructure issue to get yourself added. Under the dir activemq, there is a directory activemq-cpp/source for the source distributions of ActiveMQ-CPP.

Under the source dir, we just drop the archive files (*.zip, *.tar.gz, *.zip.asc, *.tar.gz.asc) for the release versions. (NOTE: After you add files here, you should wait about 24 hours before notifying the lists since it takes a while for all the mirrors to pick up the files).

The links on our download pages reference an apache CGI script to handle the mirrors. This is a quick and dirty way of doing this, but it works until we come up with a better way. An example usage of the link is the url below:

[http://www.apache.org/dyn/closer.cgi/activemq/activemq-cpp/source/activemq-cpp-1.1-src.zip](http://www.apache.org/dyn/closer.cgi/activemq/activemq-cpp/source/activemq-cpp-1.1-src.zip)

The cgi script "closer.cgi" takes a file resource relative to www/www.apache.org/dist and generates a download page for it with the list of mirrors.

To see this in action, go to the amq-cpp 1.1 download page here: [activemq-cpp-11-release.md](/components/cms/download/11-release)

... and click on one of the archives. You'll be taken to a generic-looking download page.

Like I said, this is a quick and dirty for now, but it works!

### Product version, interface version and package name

The page [ActiveMQ-CPP product version number](/components/cms/download/activemq-cpp-product-version-number) has been created to specify the way version numbers are used in the project, with some examples.

[ActiveMQ-CPP, libtool and packaging notes](/components/cms/download/libtool-and-packaging-notes) discusses the way version numbers impact libtool and packaging, and make some recommendations for this project.

### Creating a Distribution (step-by-step)

*   Create a pre-release download page for your distribution. This should have a disclaimer that the release is not yet official and should have a temporary link to a pre-release distribution. For the pre-release, you do not need to have both zip and tar.gz, but a detached signature file (.asc) file should accompany any distribution. Below is an example of the disclaimer that should appear at the top of the download page:

    > The Release is still in progress
    > 
    > You are previewing the release page for unreleased version of yadda. The download links on the page below may not work until it is officially released.
    > 
    > Until the release is approved you could try the current source bundle:
    > [http://people.apache.org/~myaccount/yadda.zip](http://people.apache.org/~myaccount/yadda.zip)

*   Call a vote on the release of the distribution. This email typically has
    ```
    [VOTE]
    ```
    in the subject line and should provide a link to the download page and the pre-release distribution.
*   Wait 3 days. If there >= 3 +1's and no -1's, you can proceed with the release.
*   Create all of the distributions of the source (*.zip, *.tar.gz). Make sure that you remove the .svn directories from the directory before you create the archives. On *nix, this can be done with the command:
    ```
    rm -rf \`find . -type d -name .svn\`
    ````
*   Sign your distribution files, creating an asc file for both the tar and zip files, see this page [http://www.apache.org/dev/release-signing.html](http://www.apache.org/dev/release-signing.html) for more information on release signing.
    ```
    gpg --armor --output foo.tar.gz.asc --detach-sig foo.tar.gz
    gpg --armor --output foo.zip.asc --detach-sig foo.zip
    ````
*   Copy the distribution files to **/www/www.apache.org/dist/activemq/activemq-cpp/source** on minotaur.
*   Wait 24 hours for the mirrors to be updated with the distribution.
*   Update the links on the download page to reference your distribution through the CGI script. This simply means that you precede the file names with the path [http://www.apache.org/dyn/closer.cgi/activemq/activemq-cpp/source/](http://www.apache.org/dyn/closer.cgi/activemq/activemq-cpp/source/).
    ```
    i.e. http://www.apache.org/dyn/closer.cgi/activemq/activemq-cpp/source/activemq-cpp-1.1-src.zip
    ```
*   Generate the Doxygen API and place it under /www/activemq.apache.org/cms/api_docs.
*   Add the link to the release API to the [API](/components/cms/documentation#api) page.
*   Add a news item under the CMS space on the wiki about the release.
*   Send out an e-mail on both the dev and users list about the release.

