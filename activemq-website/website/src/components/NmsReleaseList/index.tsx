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
import NmsDownloadTable, {type NmsProduct} from '../NmsDownloadTable';
import {
  nmsAmqpCurrentPrefixes,
  nmsAmqpReleases,
  nmsApiCurrentPrefixes,
  nmsApiReleases,
  type NmsRelease,
} from '../../data/nmsReleases';
import {formatDate} from '@site/src/utils/formatDate';
import {currentReleases, pastReleases} from '@site/src/utils/releases';

interface Product {
  /** Heading prefix of each release. */
  label: string;
  releases: NmsRelease[];
  prefixes: string[];
  /**
   * When the product publishes a page per release, the base path those pages
   * live under. Releases then link to their own page instead of being
   * described inline.
   */
  releasePageBase?: string;
}

const PRODUCTS: Record<NmsProduct, Product> = {
  api: {label: 'NMS API', releases: nmsApiReleases, prefixes: nmsApiCurrentPrefixes},
  amqp: {
    label: 'NMS AMQP',
    releases: nmsAmqpReleases,
    prefixes: nmsAmqpCurrentPrefixes,
    releasePageBase: '/components/nms/providers/amqp/downloads',
  },
};

interface NmsReleaseListProps {
  product: NmsProduct;
  /** The newest release of each current stream, or all the other ones. */
  list: 'current' | 'past';
}

/** Renders a list of NMS releases together with their download links. */
export default function NmsReleaseList({product, list}: NmsReleaseListProps) {
  const {label, releases: allReleases, prefixes, releasePageBase} = PRODUCTS[product];
  const isCurrentRelease = list === 'current';
  const releases = isCurrentRelease
    ? currentReleases(allReleases, prefixes)
    : pastReleases(allReleases, prefixes);

  return (
    <>
      {releases.map((release) => (
        <div key={release.version}>
          <h4>
            {label} {release.version} ({formatDate(release.releaseDate, 'long')})
          </h4>
          {releasePageBase && release.slug ? (
            <p>
              <a href={release.releaseNotes}>Release Notes</a> |{' '}
              <Link to={`${releasePageBase}/${release.slug}`}>Release Page</Link>
            </p>
          ) : (
            <p>{release.shortDescription}</p>
          )}
          <NmsDownloadTable
            product={product}
            version={release.version}
            isCurrentRelease={isCurrentRelease}
          />
          {!releasePageBase && (
            <>
              <h6>Changelog</h6>
              <p>
                For a detailed view of new features and bug fixes, see the{' '}
                <a href={release.releaseNotes}>Release Notes</a>.
              </p>
            </>
          )}
        </div>
      ))}
    </>
  );
}
