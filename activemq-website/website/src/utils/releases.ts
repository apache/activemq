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

/*
 * Current release streams are declared as "x.y." version prefixes, see
 * src/data/currentReleases.ts and src/data/nmsReleases.ts.
 *
 * Also imported by the scripts/, so it must not import anything itself.
 */

export function isCurrentRelease(version: string, prefixes: string[]): boolean {
  return prefixes.some((prefix) => version.startsWith(prefix));
}

/**
 * The newest release of each current stream, in the order the streams are
 * declared. `releases` must be sorted newest first.
 */
export function currentReleases<T extends {version: string}>(releases: T[], prefixes: string[]): T[] {
  return prefixes
    .map((prefix) => releases.find((release) => release.version.startsWith(prefix)))
    .filter((release): release is T => release !== undefined);
}

/** Every release which is not the newest one of a current stream. */
export function pastReleases<T extends {version: string}>(releases: T[], prefixes: string[]): T[] {
  const current = new Set(currentReleases(releases, prefixes).map((release) => release.version));
  return releases.filter((release) => !current.has(release.version));
}
