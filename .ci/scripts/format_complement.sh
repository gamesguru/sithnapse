#!/usr/bin/env bash

set -euo pipefail

jq -rR --unbuffered '
  . as $raw
  | (try fromjson catch $raw)
  | if type != "object" then
      $raw
    elif (.Action == "pass" or .Action == "fail" or .Action == "skip") then
      if .Test then
        "\(.Action | ascii_upcase) \(.Package // "") \(.Test) \(.Elapsed // 0)s"
      elif .Action == "fail" then
        "FAIL PACKAGE \(.Package // "<unknown>") \(.Elapsed // 0)s"
      else
        empty
      end
    else
      empty
    end'
