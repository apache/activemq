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

import Link from '@docusaurus/Link';
import NmsDownloadTable from '../NmsDownloadTable';
import VerifyDownloads from '../VerifyDownloads';
import {nmsAmqpCurrentPrefixes, nmsAmqpReleases} from '../../data/nmsReleases';
import {currentReleases} from '@site/src/utils/releases';

interface NmsAmqpReleaseProps {
  version: string;
  children?: React.ReactNode;
}

/**
 * Wraps the body of an Apache.NMS.AMQP release page with the download links,
 * change log and verification instructions, replacing the nms_amqp_release
 * layout of the original Jekyll site.
 *
 * This component is rendered from MDX pages, which are already wrapped in the
 * site Layout, so it must not add one itself.
 */
export default function NmsAmqpRelease({
  version,
  children,
}: NmsAmqpReleaseProps) {
  const release = nmsAmqpReleases.find((candidate) => candidate.version === version);
  const isCurrent = currentReleases(nmsAmqpReleases, nmsAmqpCurrentPrefixes).some(
    (candidate) => candidate.version === version,
  );

  return (
    <>
      <h1>Apache.NMS.AMQP {version} Release</h1>

      {!isCurrent && (
        <div className="alert alert--warning margin-bottom--md">
          This is an older release. To get the current release, please see the{' '}
          <Link to="/components/nms/providers/amqp/downloads">download page</Link>.
        </div>
      )}

      {children}

      <h2>Download</h2>
      <NmsDownloadTable product="amqp" version={version} isCurrentRelease={isCurrent} />

      {release && (
        <>
          <h2>Change Log</h2>
          <p>
            For a more detailed view of new features and bug fixes, see the{' '}
            <a href={release.releaseNotes}>release notes</a>.
          </p>
        </>
      )}

      <h2>Verify the Integrity of Downloads</h2>
      <VerifyDownloads />
    </>
  );
}
