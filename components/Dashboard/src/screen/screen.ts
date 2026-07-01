// Distribution-screen logic (fast-screen steps 3-4, design D1/D2).
// Pure and deterministic — no RNG. Given a capability's profiling stats and a
// residual pocket, produce distribution evidence + a promotion verdict.

import type {
  FeatureCapability,
  ResidualPocket,
  ScreenedField,
  ScreenVerdict,
} from '../data/types'

/** Screening thresholds (visible in the UI so the verdict is explainable). */
export const THRESHOLDS = {
  minCoverage: 0.35, // below → blocked (not enough data)
  minSeparation: 0.35, // below → weak (does not discriminate the pocket)
  maxConcentration: 0.9, // above → weak (near-constant, low information)
  driftPsi: 0.2, // above → drift flag (non-blocking)
} as const

const clamp01 = (x: number) => Math.max(0, Math.min(1, x))

/**
 * Pocket-relative subgroup separation (design D1): a field's intrinsic
 * separation is boosted when its family is one the pocket proposes, and
 * damped otherwise — so different pockets surface different fields.
 */
export function pocketSeparation(cap: FeatureCapability, pocket?: ResidualPocket): number {
  if (!pocket) return clamp01(cap.baseSeparation)
  const relevant = pocket.proposedFamilies.includes(cap.family)
  return clamp01(cap.baseSeparation * (relevant ? 1.25 : 0.75))
}

export function screenField(cap: FeatureCapability, pocket?: ResidualPocket): ScreenedField {
  const separation = pocketSeparation(cap, pocket)
  const drifting = cap.psi > THRESHOLDS.driftPsi
  const reasons: string[] = []

  let verdict: ScreenVerdict
  if (cap.coverage < THRESHOLDS.minCoverage) {
    verdict = 'blocked'
    reasons.push(`coverage ${(cap.coverage * 100).toFixed(0)}% below ${THRESHOLDS.minCoverage * 100}% floor`)
  } else if (cap.bucketConcentration > THRESHOLDS.maxConcentration) {
    verdict = 'weak'
    reasons.push(`near-constant (bucket concentration ${(cap.bucketConcentration * 100).toFixed(0)}%)`)
  } else if (separation < THRESHOLDS.minSeparation) {
    verdict = 'weak'
    reasons.push(`low subgroup separation (${separation.toFixed(2)}) for this pocket`)
  } else {
    verdict = 'strong'
    reasons.push(`separates the pocket (${separation.toFixed(2)}) with ${(cap.coverage * 100).toFixed(0)}% coverage`)
  }
  if (drifting) reasons.push(`PSI ${cap.psi.toFixed(2)} — distribution drift`)

  return {
    capabilityId: cap.capabilityId,
    family: cap.family,
    stats: {
      coverage: cap.coverage,
      missingness: clamp01(1 - cap.coverage),
      bucketConcentration: cap.bucketConcentration,
      klDivergence: cap.klDivergence,
      psi: cap.psi,
      subgroupSeparation: separation,
    },
    verdict,
    drifting,
    reasons,
  }
}

/** Screen a set of capabilities against a pocket, ranked strongest-evidence first. */
export function screenAndRank(caps: FeatureCapability[], pocket?: ResidualPocket): ScreenedField[] {
  const order: Record<ScreenVerdict, number> = { strong: 0, weak: 1, blocked: 2 }
  return caps
    .map((c) => screenField(c, pocket))
    .sort((a, b) => {
      if (order[a.verdict] !== order[b.verdict]) return order[a.verdict] - order[b.verdict]
      return b.stats.subgroupSeparation - a.stats.subgroupSeparation
    })
}

export const canPromote = (f: ScreenedField): boolean => f.verdict === 'strong'
