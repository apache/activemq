/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import {FontAwesomeIcon} from '@fortawesome/react-fontawesome';
import {faDownload} from '@fortawesome/free-solid-svg-icons';
import releases from '@site/src/data/generated/releases.json';
import {formatDate} from '@site/src/utils/formatDate';
import {currentReleases, isCurrentRelease} from '@site/src/utils/releases';
import VerifyDownloads from '@site/src/components/VerifyDownloads';
import {activeReleasePrefixes, currentReleasePrefixes} from '@site/src/data/currentReleases';

// Generated from the release pages by scripts/generate-release-news.ts, newest first.
const classicReleases = releases.filter((release) => release.component === 'classic');

/** The newest release of each current series, in the order they are declared. */
const currentClassicReleases = currentReleases(classicReleases, currentReleasePrefixes);

/** Past series still listed in the summary table, after the current ones. */
const summaryPastReleasePrefixes = ['5.18.'];

/** The newest release of each series of the summary table. */
const summaryReleases = currentReleases(classicReleases, [...currentReleasePrefixes, ...summaryPastReleasePrefixes]);

function seriesOf(version: string): string {
  return version.split('.').slice(0, 2).join('.');
}

function isActive(version: string): boolean {
  return isCurrentRelease(version, activeReleasePrefixes);
}

function Status({version}: {version: string}) {
  return isActive(version) ? <strong>Active</strong> : <em>Inactive</em>;
}

/** Current releases are downloaded from the mirrors, older ones from the archive. */
function binaryUrl(version: string, extension: 'zip' | 'tar.gz'): string {
  const file = `apache-activemq-${version}-bin.${extension}`;
  return isCurrentRelease(version, currentReleasePrefixes)
    ? `https://www.apache.org/dyn/closer.cgi?filename=/activemq/${version}/${file}&action=download`
    : `https://archive.apache.org/dist/activemq/${version}/${file}`;
}

type SeriesSchedule = {
  series: string;
  brokerJms: string;
  clientJms: string;
  java: string;
  spring: string;
  logging: string;
  web: string;
  /** Latest release of the series, when it has no release page. */
  last?: string;
  next?: string;
  eta?: string;
};

const schedule: SeriesSchedule[] = [
  { series: '6.3', brokerJms: 'Jakarta JMS 2/3.1 (partial)', clientJms: 'Jakarta JMS 2/3.1', java: '[17,26)', spring: '7.0.8', logging: 'Log4j 2.26.1/Slf4j 2.0.18', web: 'Jetty 12.1.12', next: '6.3.3' },
  { series: '6.2', brokerJms: 'Jakarta JMS 2/3.1 (partial)', clientJms: 'Jakarta JMS 2/3.1', java: '[17,23)', spring: '6.2.19', logging: 'Log4j 2.25.4/Slf4j 2.0.17', web: 'Jetty 11.0.26' },
  { series: '6.1', brokerJms: 'Jakarta JMS 2/3.1 (partial)', clientJms: 'Jakarta JMS 2/3.1', java: '[17,23)', spring: '6.1.21', logging: 'Log4j 2.25.2/Slf4j 2.0.17', web: 'Jetty 11.0.26' },
  { series: '6.0', brokerJms: 'Jakarta JMS 2/3.1 (partial)', clientJms: 'Jakarta JMS 2/3.1', java: '[17,23)', spring: '6.0.17', logging: 'Log4j 2.22.0/Slf4j 2.0.9', web: 'Jetty 11.0.18' },
  { series: '5.19', brokerJms: 'Javax JMS 1.1', clientJms: 'Javax JMS 1.1/Jakarta JMS 2', java: '[11,23)', spring: '5.3.39', logging: 'Log4j 2.25.3/Slf4j 2.0.17', web: 'Jetty 9.4.58.v20250814', next: '5.19.12' },
  { series: '5.18', brokerJms: 'Javax JMS 1.1', clientJms: 'Javax JMS 1.1/Jakarta JMS 2', java: '[11,23)', spring: '5.3.39', logging: 'Log4j 2.24.1/Slf4j 2.0.13', web: 'Jetty 9.4.57.v20241219' },
  { series: '5.17', brokerJms: 'Javax JMS 1.1', clientJms: 'Javax JMS 1.1', java: '[11,23)', spring: '5.3.33', logging: 'Log4j 2.23.1/Slf4j 1.7.36', web: 'Jetty 9.4.54.v20240208' },
  { series: '5.16', brokerJms: 'Javax JMS 1.1', clientJms: 'Javax JMS 1.1', java: '1.8', spring: '4.3.30.RELEASE', logging: 'Reload4j 1.2.24/Slf4j 1.7.36', web: 'Jetty 9.4.53.v20231009' },
  { series: '5.15', brokerJms: 'Javax JMS 1.1', clientJms: 'Javax JMS 1.1', java: '1.8', spring: '4.3.30.RELEASE', logging: 'Log4j 1.2.17/Slf4j 1.7.32', web: 'Jetty 9.4.39.v20210325' },
  { series: '5.14', brokerJms: 'Javax JMS 1.1', clientJms: 'Javax JMS 1.1', java: '1.7', spring: '4.1.9.RELEASE', logging: 'Log4j 1.2.17/Slf4j 1.7.13', web: 'Jetty 9.2.13.v20150730', last: '5.14.5' },
  { series: '5.13', brokerJms: 'Javax JMS 1.1', clientJms: 'Javax JMS 1.1', java: '1.7', spring: '4.1.9.RELEASE', logging: 'Log4j 1.2.17/Slf4j 1.7.13', web: 'Jetty 9.2.13.v20150730', last: '5.13.5' },
  { series: '5.12', brokerJms: 'Javax JMS 1.1', clientJms: 'Javax JMS 1.1', java: '1.7', spring: '3.2.16.RELEASE', logging: 'Log4j 1.2.17/Slf4j 1.7.10', web: 'Jetty 9.2.6.v20141205', last: '5.12.3' },
  { series: '5.11', brokerJms: 'Javax JMS 1.1', clientJms: 'Javax JMS 1.1', java: '1.7', spring: '3.2.16.RELEASE', logging: 'Log4j 1.2.17/Slf4j 1.7.10', web: 'Jetty 9.2.6.v20141205', last: '5.11.4' },
  { series: '5.10', brokerJms: 'Javax JMS 1.1', clientJms: 'Javax JMS 1.1', java: '1.6', spring: '3.2.8.RELEASE', logging: 'Log4j 1.2.17/Slf4j 1.7.5', web: 'Jetty 7.6.9.v20130131' },
];

/** The latest release of a series, from its release pages. */
function lastRelease({series, last}: SeriesSchedule): string {
  return classicReleases.find((release) => seriesOf(release.version) === series)?.version ?? last ?? '';
}

export default function DownloadPage() {
  return (
    <Layout title="Download ActiveMQ">
      <div className="container margin-vert--lg">
        <h1>Download ActiveMQ</h1>

        <h4>Summary Table of ActiveMQ Series Status</h4>
        <table>
          <thead>
            <tr>
              <th>Series</th>
              <th>Status</th>
              <th>Latest Patch Version</th>
              <th>Date of Release</th>
              <th>Unix</th>
              <th>Win64</th>
            </tr>
          </thead>
          <tbody>
            {summaryReleases.map((release) => (
              <tr key={release.version} style={{backgroundColor: isActive(release.version) ? '#dff0d8' : '#f0f0f0'}}>
                <td>{seriesOf(release.version)}.x</td>
                <td><Status version={release.version} /></td>
                <td>{release.version}</td>
                <td>{formatDate(release.releaseDate)}</td>
                <td><a href={binaryUrl(release.version, 'tar.gz')} title="Download UNIX"><FontAwesomeIcon icon={faDownload} /></a></td>
                <td><a href={binaryUrl(release.version, 'zip')} title="Download Win64"><FontAwesomeIcon icon={faDownload} /></a></td>
              </tr>
            ))}
          </tbody>
        </table>

        <p>
          These are the current ActiveMQ releases. For prior releases, please see the{' '}
          <Link to="/components/classic/documentation/download-archives">past releases</Link> page.
        </p>
        <p>
          It is important to <a href="#verify-the-integrity-of-downloads">verify the integrity</a> of the files you download.
        </p>

        <h5>Status Descriptions</h5>
        <p><strong>Active</strong>: Actively supported and recommended for production use. This version receives regular community updates, including new features, security patches, and bug fixes.</p>
        <p><strong>Inactive</strong>: Reached end-of-life and is no longer actively maintained. Inactive versions do not receive updates. Not recommended for new deployments; users are encouraged to upgrade to an active version for ongoing community releases.</p>

        <h4>Schedule &amp; Status</h4>
        <table>
          <thead>
            <tr>
              <th>Series</th>
              <th>Broker JMS API Support</th>
              <th>Client JMS API Client</th>
              <th>Java Version</th>
              <th>Spring Version</th>
              <th>Logging Support</th>
              <th>Web Support</th>
              <th>Status</th>
              <th>Last</th>
              <th>Next</th>
              <th>ETA</th>
            </tr>
          </thead>
          <tbody>
            {schedule.map((s) => (
              <tr key={s.series}>
                <td>{s.series}.x</td>
                <td>{s.brokerJms}</td>
                <td>{s.clientJms}</td>
                <td>{s.java}</td>
                <td>{s.spring}</td>
                <td>{s.logging}</td>
                <td>{s.web}</td>
                <td><Status version={`${s.series}.`} /></td>
                <td>{lastRelease(s)}</td>
                <td>{s.next}</td>
                <td>{s.eta}</td>
              </tr>
            ))}
          </tbody>
        </table>

        {currentClassicReleases.map((release) => (
          <div key={release.version}>
            <h4>ActiveMQ {release.version} ({formatDate(release.releaseDate)})</h4>
            <p>
              <a href={release.releaseNotes}>Release Notes</a> |{' '}
              <Link to={release.url}>Release Page</Link> |{' '}
              <Link to="/components/classic/documentation">Documentation</Link> |{' '}
              Java compatibility: <strong>{release.javaVersion}</strong>
            </p>
            <table>
              <thead>
                <tr>
                  <th>Platform</th>
                  <th>Download</th>
                  <th>SHA512</th>
                  <th>Signature</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>Windows</td>
                  <td><a href={`https://www.apache.org/dyn/closer.cgi?filename=/activemq/${release.version}/apache-activemq-${release.version}-bin.zip&action=download`}>apache-activemq-{release.version}-bin.zip</a></td>
                  <td><a href={`https://downloads.apache.org/activemq/${release.version}/apache-activemq-${release.version}-bin.zip.sha512`}>SHA512</a></td>
                  <td><a href={`https://downloads.apache.org/activemq/${release.version}/apache-activemq-${release.version}-bin.zip.asc`}>ASC</a></td>
                </tr>
                <tr>
                  <td>Unix/Linux/Cygwin</td>
                  <td><a href={`https://www.apache.org/dyn/closer.cgi?filename=/activemq/${release.version}/apache-activemq-${release.version}-bin.tar.gz&action=download`}>apache-activemq-{release.version}-bin.tar.gz</a></td>
                  <td><a href={`https://downloads.apache.org/activemq/${release.version}/apache-activemq-${release.version}-bin.tar.gz.sha512`}>SHA512</a></td>
                  <td><a href={`https://downloads.apache.org/activemq/${release.version}/apache-activemq-${release.version}-bin.tar.gz.asc`}>ASC</a></td>
                </tr>
                <tr>
                  <td>Source</td>
                  <td><a href={`https://www.apache.org/dyn/closer.cgi?filename=/activemq/${release.version}/activemq-parent-${release.version}-source-release.zip&action=download`}>activemq-parent-{release.version}-source-release.zip</a></td>
                  <td><a href={`https://downloads.apache.org/activemq/${release.version}/activemq-parent-${release.version}-source-release.zip.sha512`}>SHA512</a></td>
                  <td><a href={`https://downloads.apache.org/activemq/${release.version}/activemq-parent-${release.version}-source-release.zip.asc`}>ASC</a></td>
                </tr>
              </tbody>
            </table>
          </div>
        ))}

        <hr />
        <h4 id="verify-the-integrity-of-downloads">Verify the Integrity of Downloads</h4>
        <VerifyDownloads pgp={false} />
      </div>
    </Layout>
  );
}
