// The four reviewed, fixed dimension families (TRD §7.4, design D4).
// Arbitrary dimensions are disallowed; the Aggregation Builder only offers these.

import type { DimensionFamily, DimensionFamilyId } from '../data/types'

export const DIMENSION_FAMILIES: DimensionFamily[] = [
  {
    id: 'device_level_v1',
    purpose: 'User/device history features.',
    keys: ['lo_id_or_device_id', 'dev_platform', 'placement_type', 'geoip_country_code'],
    optionalBuckets: ['pub_app_object_id_top_bucket'],
    costTier: 'highest',
    notes: [
      'Highest row count — strict sample required for demo.',
      'Limited measure count; no high-cardinality extra dimensions.',
    ],
  },
  {
    id: 'non_device_context_v1',
    purpose: 'Main non-device feature plane: all important context except device identity.',
    keys: [
      'pub_app_object_id',
      'placement_id',
      'placement_type',
      'pub_account_id',
      'supply_name',
      'is_header_bidding',
      'rtb_connection_id',
      'winner_account_id',
      'jaeger_experiment_id',
      'ml_experiment_id',
      'geoip_country_code',
      'dev_platform',
      'os_version_major',
      'dev_model_bucket',
      'dev_connection',
      'dev_id_source',
    ],
    costTier: 'high',
    notes: [
      'dev_model_bucket must be top-N bucketed, not raw model.',
      'adomain not included by default; use a topk_map primitive.',
    ],
  },
  {
    id: 'inventory_context_lite_v1',
    purpose: 'Fallback and cost-safe broad coverage.',
    keys: [
      'pub_app_object_id',
      'placement_id',
      'placement_type',
      'supply_name',
      'rtb_connection_id',
      'geoip_country_code',
      'dev_platform',
    ],
    costTier: 'medium',
    notes: ['Fast preview / broad simulation / backoff when non_device is sparse.'],
  },
  {
    id: 'global_baseline_v1',
    purpose: 'Low-cardinality priors and fallback.',
    keys: ['placement_type', 'supply_name', 'rtb_connection_id', 'geoip_country_code', 'dev_platform'],
    costTier: 'low',
    notes: ['Cold start / null backfill / cheap always-on comparison.'],
  },
]

export const DIMENSION_FAMILY_IDS: DimensionFamilyId[] = DIMENSION_FAMILIES.map((f) => f.id)

/** All keys allowed by any reviewed family — the closed set of legal dimensions. */
export const ALLOWED_DIMENSION_KEYS: ReadonlySet<string> = new Set(
  DIMENSION_FAMILIES.flatMap((f) => [...f.keys, ...(f.optionalBuckets ?? [])]),
)

/** Suggest the closest family that supports a given key (for the "arbitrary dim" guardrail). */
export function familiesSupportingKey(key: string): DimensionFamilyId[] {
  return DIMENSION_FAMILIES.filter(
    (f) => f.keys.includes(key) || (f.optionalBuckets ?? []).includes(key),
  ).map((f) => f.id)
}
