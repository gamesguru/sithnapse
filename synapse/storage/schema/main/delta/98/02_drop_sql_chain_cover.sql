-- The SQL chain-cover index is retired. Auth-chain traversals use the direct
-- SQL event_auth graph when mtxdb is disabled, and the mtxdb auth graph when
-- it is enabled; neither path reads these derived chain-cover tables.
DROP TABLE IF EXISTS event_auth_chain_links;
DROP TABLE IF EXISTS event_auth_chain_to_calculate;
DROP TABLE IF EXISTS event_auth_chains;
