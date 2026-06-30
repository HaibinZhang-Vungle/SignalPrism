# WideTable

This component owns the realtime attributed wide-table layer for the demo.

Its main responsibility is to turn event-level attribution sources into a
single impression-grain table that downstream systems can profile, aggregate,
and join to labels. In the TRD, this corresponds to the `Realtime attributed
wide table` at the start of the end-to-end MLOps flow.

Core functions:

- Join and normalize Jaeger and HB/S2S event data into
  `ml_shadow.realtime_attributed_event_wide`.
- Preserve event-grain keys such as `event_id`, `imp_id`, and transaction ids
  for attribution, label joins, and GMinor log joins.
- Expose column metadata needed by the capability scanner, including semantic
  type, null semantics, feature suitability, and enum references.
- Keep label and leakage guidance close to the wide-table contract so offline
  simulations can enforce point-in-time correctness.

The wide table should remain an event-level source of truth. Aggregation
dimensions and historical feature tables belong in the Data component.

