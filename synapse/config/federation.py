#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright 2020 The Matrix.org Foundation C.I.C.
# Copyright (C) 2023 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
# Originally licensed under the Apache License, Version 2.0:
# <http://www.apache.org/licenses/LICENSE-2.0>.
#
# [This file includes modifications made by New Vector Limited]
#
#
from typing import Any

from synapse.config._base import Config, ConfigError
from synapse.config._util import validate_config
from synapse.types import JsonDict


class FederationConfig(Config):
    section = "federation"

    def read_config(self, config: JsonDict, **kwargs: Any) -> None:
        federation_config = config.setdefault("federation", {})

        # FIXME: federation_domain_whitelist needs sytests
        self.federation_domain_whitelist: dict | None = None
        federation_domain_whitelist = config.get("federation_domain_whitelist", None)

        if federation_domain_whitelist is not None:
            # turn the whitelist into a hash for speed of lookup
            self.federation_domain_whitelist = {}

            for domain in federation_domain_whitelist:
                self.federation_domain_whitelist[domain] = True

        self.federation_whitelist_endpoint_enabled = config.get(
            "federation_whitelist_endpoint_enabled", False
        )

        federation_metrics_domains = config.get("federation_metrics_domains") or []
        validate_config(
            _METRICS_FOR_DOMAINS_SCHEMA,
            federation_metrics_domains,
            ("federation_metrics_domains",),
        )
        self.federation_metrics_domains = set(federation_metrics_domains)

        self.allow_profile_lookup_over_federation = config.get(
            "allow_profile_lookup_over_federation", True
        )

        self.allow_device_name_lookup_over_federation = config.get(
            "allow_device_name_lookup_over_federation", False
        )

        # Partial-state joins reduce the amount of room state needed during
        # federation joins. Some deployments may prefer to always request the
        # full room state instead.
        self.allow_partial_state = federation_config.get("allow_partial_state", True)

        # Event IDs the operator has decided to accept even though their origin
        # signature cannot be verified (see `_check_sigs_and_hash`).
        accept_unverifiable = federation_config.get("accept_unverifiable_event_ids", [])
        if not isinstance(accept_unverifiable, list) or not all(
            isinstance(e, str) and e.startswith("$") for e in accept_unverifiable
        ):
            raise ConfigError(
                "federation.accept_unverifiable_event_ids must be a list of "
                "event IDs (strings starting with '$')",
                ("federation", "accept_unverifiable_event_ids"),
            )
        self.accept_unverifiable_event_ids: frozenset[str] = frozenset(
            accept_unverifiable
        )

        # Allow for the configuration of timeout, max request retries
        # and min/max retry delays in the matrix federation client.
        self.client_timeout_ms = Config.parse_duration(
            federation_config.get("client_timeout", "60s")
        )
        self.max_long_retry_delay_ms = Config.parse_duration(
            federation_config.get("max_long_retry_delay", "60s")
        )
        self.max_short_retry_delay_ms = Config.parse_duration(
            federation_config.get("max_short_retry_delay", "2s")
        )
        self.max_long_retries = federation_config.get("max_long_retries", 10)
        self.max_short_retries = federation_config.get("max_short_retries", 3)

        # Allow for the configuration of the backoff algorithm used
        # when trying to reach an unavailable destination.
        # Unlike previous configuration those values applies across
        # multiple requests and the state of the backoff is stored on DB.
        self.destination_min_retry_interval_ms = Config.parse_duration(
            federation_config.get("destination_min_retry_interval", "10m")
        )
        self.destination_retry_multiplier = federation_config.get(
            "destination_retry_multiplier", 2
        )
        self.destination_max_retry_interval_ms = min(
            Config.parse_duration(
                federation_config.get("destination_max_retry_interval", "7d")
            ),
            # Set a hard-limit to not overflow the database column.
            2**62,
        )

    def is_domain_allowed_according_to_federation_whitelist(self, domain: str) -> bool:
        """
        Returns whether a domain is allowed according to the federation whitelist. If a
        federation whitelist is not set, all domains are allowed.

        Args:
            domain: The domain to test.

        Returns:
            True if the domain is allowed or if a whitelist is not set, False otherwise.
        """
        if self.federation_domain_whitelist is None:
            return True

        return domain in self.federation_domain_whitelist


_METRICS_FOR_DOMAINS_SCHEMA = {"type": "array", "items": {"type": "string"}}
