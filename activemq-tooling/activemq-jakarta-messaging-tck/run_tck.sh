#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

# Force C/POSIX locale to avoid issues with locale-specific number formatting
# (e.g. French locale uses comma as decimal separator which breaks JavaTest harness)
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8

JTE_FILE="${1:?Usage: run_tck.sh <ts.jte>}"

TCK_VERSION="3.1.0"
TCK_SPEC_VERSION="3.1"
TCK_ZIP="jakarta-messaging-tck-${TCK_VERSION}.zip"
TCK_URL="https://download.eclipse.org/jakartaee/messaging/${TCK_SPEC_VERSION}/${TCK_ZIP}"
TCK_SHA256="3ea9e4d9eb6c7ebd2f60f920c1e76ea2e2928540b0eee78120a18453d5851644"

TARGET_DIR="$(pwd)/target"
TCK_DIR="${TARGET_DIR}/messaging-tck"
SHADED_JAR=$(find "${TARGET_DIR}" -maxdepth 1 -name "activemq-jakarta-messaging-tck-*.jar" ! -name "*-sources*" ! -name "*-javadoc*" ! -name "original-*" | head -1)

if [ -z "${SHADED_JAR}" ]; then
    echo "ERROR: Shaded JAR not found in ${TARGET_DIR}. Run 'mvn package' first."
    exit 1
fi
echo "Using shaded JAR: ${SHADED_JAR}"

# Download TCK if not already present
if [ ! -f "${TCK_ZIP}" ]; then
    echo "Downloading Jakarta Messaging TCK ${TCK_VERSION}..."
    curl -fSL -o "${TCK_ZIP}" "${TCK_URL}"
fi

# Verify checksum
echo "Verifying SHA-256 checksum..."
if command -v sha256sum &>/dev/null; then
    ACTUAL_SHA256=$(sha256sum "${TCK_ZIP}" | awk '{print $1}')
else
    ACTUAL_SHA256=$(shasum -a 256 "${TCK_ZIP}" | awk '{print $1}')
fi
if [ "${ACTUAL_SHA256}" != "${TCK_SHA256}" ]; then
    echo "ERROR: SHA-256 checksum mismatch!"
    echo "  Expected: ${TCK_SHA256}"
    echo "  Actual:   ${ACTUAL_SHA256}"
    echo "Deleting corrupt download."
    rm -f "${TCK_ZIP}"
    exit 1
fi
echo "Checksum verified."

# Extract TCK
if [ ! -d "${TCK_DIR}" ]; then
    echo "Extracting TCK to ${TCK_DIR}..."
    mkdir -p "${TCK_DIR}"
    unzip -q -o "${TCK_ZIP}" -d "${TCK_DIR}"
fi

# Find the extracted TCK root (may be nested in a subdirectory)
TCK_ROOT=$(find "${TCK_DIR}" -mindepth 1 -maxdepth 1 -type d -name "messaging-tck" | head -1)
if [ -z "${TCK_ROOT}" ]; then
    # Try without nesting
    if [ -d "${TCK_DIR}/bin" ]; then
        TCK_ROOT="${TCK_DIR}"
    else
        echo "ERROR: Could not locate TCK root directory under ${TCK_DIR}"
        ls -la "${TCK_DIR}"
        exit 1
    fi
fi
echo "TCK root: ${TCK_ROOT}"

# Copy JTE and JTX configuration files
echo "Configuring TCK..."
cp "${JTE_FILE}" "${TCK_ROOT}/bin/ts.jte"
if [ -f "ts.jtx" ]; then
    cp "ts.jtx" "${TCK_ROOT}/bin/ts.jtx"
fi

# Append dynamic paths to ts.jte
{
    echo ""
    echo "jms.home=${TCK_ROOT}"
    echo "jms.classes=${SHADED_JAR}"
} >> "${TCK_ROOT}/bin/ts.jte"

# Ensure Ant is available (use system Ant; TCK does not bundle one)
if ! command -v ant &>/dev/null; then
    echo "ERROR: Apache Ant is required but not found on PATH."
    echo "Install it with: apt-get install ant  (or equivalent for your OS)"
    exit 1
fi

# ANT_HOME must be set for the TCK harness classpath (${ant.home}/lib/ant.jar)
if [ -z "${ANT_HOME:-}" ]; then
    # Derive ANT_HOME from the ant binary location
    ANT_BIN="$(command -v ant)"
    ANT_REAL="$(readlink -f "${ANT_BIN}" 2>/dev/null || realpath "${ANT_BIN}" 2>/dev/null || echo "${ANT_BIN}")"
    export ANT_HOME="${ANT_REAL%/bin/ant}"
    # On Debian/Ubuntu, ant is a shell script in /usr/bin but jars are in /usr/share/ant
    if [ ! -f "${ANT_HOME}/lib/ant.jar" ] && [ -f "/usr/share/ant/lib/ant.jar" ]; then
        export ANT_HOME="/usr/share/ant"
    fi
fi

echo "Using ANT_HOME: ${ANT_HOME}"
echo "Ant version: $(ant -version)"

# Create required temp directories and files
mkdir -p /tmp/ri_admin_objects "${TARGET_DIR}/tck-work" "${TARGET_DIR}/tck-report" 2>/dev/null || true
# Create password file for RI porting compatibility
echo "admin" > /tmp/ripassword

# Patch TCK source for JDK 17+ compatibility
# javax.rmi.PortableRemoteObject was removed in JDK 14+
TSNAMING="${TCK_ROOT}/src/com/sun/ts/lib/util/TSNamingContext.java"
if grep -q "javax.rmi.PortableRemoteObject" "${TSNAMING}" 2>/dev/null; then
    echo "Patching TSNamingContext.java for JDK 17+ compatibility..."
    sed -i.bak 's|import javax\.rmi\.PortableRemoteObject;|// Removed: javax.rmi unavailable on JDK 17+|' "${TSNAMING}"
    sed -i.bak 's|return c == null ? o : PortableRemoteObject\.narrow(o, c);|return c == null ? o : c.cast(o);|' "${TSNAMING}"
    rm -f "${TSNAMING}.bak"
fi

# Build the TCK test classes first
echo ""
echo "============================================"
echo "Building Jakarta Messaging TCK ${TCK_VERSION}"
echo "============================================"
echo ""

cd "${TCK_ROOT}/bin"
ant build.all 2>&1 | tail -20
echo "TCK build complete."

# Run the TCK tests using same-JVM mode to avoid security manager issues on Java 17+
echo ""
echo "============================================"
echo "Running Jakarta Messaging TCK ${TCK_VERSION}"
echo "============================================"
echo ""

cd "${TCK_ROOT}/src/com/sun/ts/tests/jms"
ant \
    -Dwork.dir="${TARGET_DIR}/tck-work" \
    -Dreport.dir="${TARGET_DIR}/tck-report" \
    runclient
TCK_EXIT=$?

echo ""
echo "============================================"
echo "TCK execution finished with exit code: ${TCK_EXIT}"
echo "Report: ${TARGET_DIR}/tck-report"
echo "============================================"

exit ${TCK_EXIT}
