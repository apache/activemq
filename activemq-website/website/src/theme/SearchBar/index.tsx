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

import React, {useMemo} from 'react';
import SearchBar from '@theme-original/SearchBar';
import {useLocation} from '@docusaurus/router';
import {useAllDocsData} from '@docusaurus/plugin-content-docs/client';

function removeTrailingSlash(path: string): string {
  return path.length > 1 ? path.replace(/\/$/, '') : path;
}

// Only docs pages are indexed, so the search bar is only shown on them. The
// docs plugins' route prefixes also cover regular pages (e.g. /components/nms/download),
// hence matching against the actual doc permalinks.
export default function SearchBarWrapper(): React.JSX.Element | null {
  const {pathname} = useLocation();
  const allDocsData = useAllDocsData();
  const docPaths = useMemo(
    () =>
      new Set(
        Object.values(allDocsData).flatMap((plugin) =>
          plugin.versions.flatMap((version) => version.docs.map((doc) => removeTrailingSlash(doc.path))),
        ),
      ),
    [allDocsData],
  );
  return docPaths.has(removeTrailingSlash(pathname)) ? <SearchBar /> : null;
}
