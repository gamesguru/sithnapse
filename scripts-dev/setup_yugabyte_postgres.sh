#!/bin/bash
#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#

set -e

echo "================================================================="
echo "YugabyteDB (PostgreSQL-on-RocksDB) NATIVE Linux Setup for Synapse"
echo "================================================================="

VERSION="2.20.1.0"
BUILD="b141"
TARBALL="yugabyte-${VERSION}-${BUILD}-linux-x86_64.tar.gz"
DOWNLOAD_URL="https://downloads.yugabyte.com/releases/${VERSION}/${TARBALL}"
INSTALL_DIR="$HOME/yugabyte-${VERSION}"
DATA_DIR="$HOME/yugabyte-data"

DB_USER="synapse_user"
DB_PASS="synapse_password"
DB_NAME="synapse"

# 1. Download and Extract YugabyteDB
if [ ! -d "${INSTALL_DIR}" ]; then
    echo "[*] Downloading YugabyteDB v${VERSION} native package..."
    if ! command -v wget &> /dev/null; then
        if ! command -v curl &> /dev/null; then
            echo "[-] Error: Neither wget nor curl is installed. Please install one to download the packages."
            exit 1
        else
            curl -L -O "${DOWNLOAD_URL}"
        fi
    else
        wget -c "${DOWNLOAD_URL}"
    fi

    echo "[*] Extracting package to ${INSTALL_DIR}..."
    mkdir -p "${INSTALL_DIR}"
    tar -xf "${TARBALL}" -C "${INSTALL_DIR}" --strip-components=1
    rm "${TARBALL}"

    echo "[*] Running YugabyteDB post-install configuring script..."
    cd "${INSTALL_DIR}"
    ./bin/post_install.sh
    cd -
else
    echo "[+] YugabyteDB already extracted in ${INSTALL_DIR}."
fi

# 2. Start YugabyteDB Native Daemon
echo "[*] Starting local native YugabyteDB single-node cluster..."
mkdir -p "${DATA_DIR}"
if ! "${INSTALL_DIR}/bin/yugabyted" status &> /dev/null; then
    "${INSTALL_DIR}/bin/yugabyted" start --base_dir="${DATA_DIR}"
else
    echo "[+] YugabyteDB daemon is already running."
fi

echo "[*] Waiting 10 seconds for SQL layer to initialize..."
sleep 10

# 3. Create database and users natively
echo "[*] Configuring database, user, and password..."
"${INSTALL_DIR}/bin/ysqlsh" \
    -h 127.0.0.1 -p 5433 -U postgres \
    -c "CREATE USER ${DB_USER} WITH PASSWORD '${DB_PASS}';" \
    -c "CREATE DATABASE ${DB_NAME} OWNER ${DB_USER};" || true

echo ""
echo "[+] YugabyteDB is up and running NATIVELY on port 5433!"
echo "    Local Data Directory: ${DATA_DIR}"
echo "    Admin UI Console: http://127.0.0.1:7000"
echo ""
echo "================================================================="
echo "HOW TO CONFIGURE MATRIX SYNAPSE (homeserver.yaml):"
echo "================================================================="
echo "Update your homeserver.yaml database block as follows:"
echo ""
echo "database:"
echo "  name: psycopg2"
echo "  args:"
echo "    user: ${DB_USER}"
echo "    password: ${DB_PASS}"
echo "    database: ${DB_NAME}"
echo "    host: 127.0.0.1"
echo "    port: 5433"
echo "    cp_min: 5"
echo "    cp_max: 10"
echo "================================================================="
