#!/bin/bash
#
# Default ENTRYPOINT for the docker image used for testing synapse with workers under complement

set -e

# scripts-dev/complement.sh points the mtxdb store at a per-container
# subdirectory of a host-mounted directory by passing a path containing
# @HOSTNAME@. Expand it here (the container hostname is unique per container)
# and hand the directory to whichever user owns /data, so Synapse can write it.
if [[ "${SYNAPSE_EMBEDDED_HAMT_PATH:-}" == *@HOSTNAME@* ]]; then
  export SYNAPSE_EMBEDDED_HAMT_PATH="${SYNAPSE_EMBEDDED_HAMT_PATH//@HOSTNAME@/$(hostname)}"
  mkdir -p "$SYNAPSE_EMBEDDED_HAMT_PATH"
  chown -R --reference=/data "$SYNAPSE_EMBEDDED_HAMT_PATH"
fi

# Per-container directory for the periodic timing snapshots (see the
# SYNAPSE_PG_TIMINGS block in scripts-dev/complement.sh).
if [[ "${SYNAPSE_TIMINGS_RUN_DIR:-}" == *@HOSTNAME@* ]]; then
  export SYNAPSE_TIMINGS_RUN_DIR="${SYNAPSE_TIMINGS_RUN_DIR//@HOSTNAME@/$(hostname)}"
  mkdir -p "$SYNAPSE_TIMINGS_RUN_DIR"
  # Synapse drops to an arbitrary UID (PASS_UID), so make this writable by any.
  chmod 777 "$SYNAPSE_TIMINGS_RUN_DIR"
fi

echo "Complement Synapse launcher"
echo "  Args: $*"
echo "  Env: SYNAPSE_COMPLEMENT_DATABASE=$SYNAPSE_COMPLEMENT_DATABASE SYNAPSE_COMPLEMENT_USE_WORKERS=$SYNAPSE_COMPLEMENT_USE_WORKERS SYNAPSE_COMPLEMENT_USE_ASYNCIO_REACTOR=$SYNAPSE_COMPLEMENT_USE_ASYNCIO_REACTOR"

function log {
    d=$(printf '%(%Y-%m-%d %H:%M:%S)T,%.3s\n' ${EPOCHREALTIME/./ })
    echo "$d $*"
}

# Set the server name of the homeserver
export SYNAPSE_SERVER_NAME=${SERVER_NAME}

# No need to report stats here
export SYNAPSE_REPORT_STATS=no


case "$SYNAPSE_COMPLEMENT_DATABASE" in
  postgres)
    # Set postgres authentication details which will be placed in the homeserver config file
    export POSTGRES_PASSWORD=somesecret
    export POSTGRES_USER=postgres
    export POSTGRES_HOST=localhost

    # configure supervisord to start postgres
    export START_POSTGRES=true
    ;;

  sqlite|"")
    # Set START_POSTGRES to false unless it has already been set
    # (i.e. by another container image inheriting our own).
    export START_POSTGRES=${START_POSTGRES:-false}
    ;;

  *)
    echo "Unknown Synapse database: SYNAPSE_COMPLEMENT_DATABASE=$SYNAPSE_COMPLEMENT_DATABASE" >&2
    exit 1
    ;;
esac


if [[ -n "$SYNAPSE_COMPLEMENT_USE_WORKERS" ]]; then
  # Specify the workers to test with
  # Allow overriding by explicitly setting SYNAPSE_WORKER_TYPES outside, while still
  # utilizing WORKERS=1 for backwards compatibility.
  # -n True if the length of string is non-zero.
  # -z True if the length of string is zero.
  if [[ -z "$SYNAPSE_WORKER_TYPES" ]]; then
    # Under mtxdb, event persistence must stay on the main process, so no
    # dedicated event_persister workers are spawned at all. workers.py's
    # embedded_hamt_engine validation requires BOTH that there is exactly
    # one events writer (a second persister would hit mtxdb's exclusive-lock
    # rejection at startup) AND that the sole writer is main itself (the
    # embedded-HAMT background migration only ever runs on main, and would
    # crash writing through a read-only-opened store otherwise). Omitting
    # event_persister leaves stream_writers.events unset, which defaults to
    # ["main"] (see WriterLocations) and satisfies both checks; a single
    # event_persister would pass the first but fail the second.
    #
    # The background_worker goes for the same reason: its generated config
    # sets run_background_tasks_on to itself, but the third embedded_hamt
    # check requires background tasks to run on main (main's own
    # background-updates poll loop runs unconditionally, and mtxdb writes
    # have no cross-instance coordination to survive a second concurrent
    # loop). Without it, the setting defaults back to main.
    event_persister_entry="event_persister:2, "
    background_worker_entry="background_worker, "
    if [[ -n "$SYNAPSE_EMBEDDED_HAMT_ENGINE" ]]; then
      event_persister_entry=""
      background_worker_entry=""
    fi
    export SYNAPSE_WORKER_TYPES="\
      ${event_persister_entry}\
      ${background_worker_entry}\
      event_creator, \
      client_reader=client_reader+user_dir, \
      media_repository, \
      federation_receiver=federation_reader+federation_inbound, \
      federation_sender, \
      synchrotron, \
      notifiers=pusher+appservice, \
      device_lists:2, \
      stream_writers=account_data+presence+receipts+to_device+typing"

  fi
  log "Workers requested: $SYNAPSE_WORKER_TYPES"
  # adjust connection pool limits on worker mode as otherwise running lots of worker synapses
  # can make docker unhappy (in GHA)
  export POSTGRES_CP_MIN=1
  export POSTGRES_CP_MAX=3
  echo "using reduced connection pool limits for worker mode"
  # Improve startup times by using a launcher based on fork()
  export SYNAPSE_USE_EXPERIMENTAL_FORKING_LAUNCHER=1
else
  # Empty string here means 'main process only'
  export SYNAPSE_WORKER_TYPES=""
fi


if [[ -n "$SYNAPSE_COMPLEMENT_USE_ASYNCIO_REACTOR" ]]; then
  if [[ -n "$SYNAPSE_USE_EXPERIMENTAL_FORKING_LAUNCHER" ]]; then
    export SYNAPSE_COMPLEMENT_FORKING_LAUNCHER_ASYNC_IO_REACTOR="1"
  else
    export SYNAPSE_ASYNC_IO_REACTOR="1"
  fi
else
  export SYNAPSE_ASYNC_IO_REACTOR="0"
fi


# Add Complement's appservice registration directory, if there is one
# (It can be absent when there are no application services in this test!)
if [ -d /complement/appservice ]; then
    export SYNAPSE_AS_REGISTRATION_DIR=/complement/appservice
fi

# Generate a TLS key, then generate a certificate by having Complement's CA sign it
# Note that both the key and certificate are in PEM format (not DER).

# First generate a configuration file to set up a Subject Alternative Name.
echo "\
.include /etc/ssl/openssl.cnf

[SAN]
subjectAltName=DNS:${SERVER_NAME}" > /conf/server.tls.conf

# Generate an RSA key
openssl genrsa -out /conf/server.tls.key 2048

# Generate a certificate signing request
openssl req -new -config /conf/server.tls.conf -key /conf/server.tls.key -out /conf/server.tls.csr \
  -subj "/CN=${SERVER_NAME}" -reqexts SAN

# Make the Complement Certificate Authority sign and generate a certificate.
openssl x509 -req -in /conf/server.tls.csr \
  -CA /complement/ca/ca.crt -CAkey /complement/ca/ca.key -set_serial 1 \
  -out /conf/server.tls.crt -extfile /conf/server.tls.conf -extensions SAN

# Assert that we have a Subject Alternative Name in the certificate.
# (the test will exit with 1 here if there isn't a SAN in the certificate.)
[[ $(openssl x509 -in /conf/server.tls.crt -noout -text) == *DNS:* ]]

export SYNAPSE_TLS_CERT=/conf/server.tls.crt
export SYNAPSE_TLS_KEY=/conf/server.tls.key

# Add a directory for tests to add config overrides if they want
mkdir --parents /conf/homeserver.d
export _SYNAPSE_COMPLEMENT_EXTRA_CONFIG_DIR=/conf/homeserver.d

# Run the script that writes the necessary config files and starts supervisord, which in turn
# starts everything else
exec /configure_workers_and_start.py "$@"
