// Formula DSL: parser -> AST + validator (design D3, TRD §7.6).
// Deliberately small and closed — no arbitrary SQL. Validates type correctness,
// division safety, primitive availability, and the point-in-time / no-future-label rule.

import type { FormulaValidation } from '../data/types'

// ---- AST -------------------------------------------------------------------

export type Ast =
  | { type: 'num'; value: number }
  | { type: 'ident'; name: string }
  | { type: 'call'; name: string; args: Ast[] }
  | { type: 'binary'; op: '+' | '-' | '*' | '/'; left: Ast; right: Ast }
  | { type: 'unary'; op: '-'; operand: Ast }

/** Allowed functions (TRD §7.6). */
export const ALLOWED_FUNCTIONS: Record<string, number | 'variadic'> = {
  safe_div: 2,
  coalesce: 'variadic',
  log1p: 1,
  sqrt: 1,
  abs: 1,
  least: 2,
  greatest: 2,
  clip: 3,
  avg: 2,
  variance: 3,
  stddev: 3,
  cv: 3,
  rate: 2,
  dd_percentile: 2,
  hll_count: 1,
  case_when: 3,
}

// ---- Tokenizer -------------------------------------------------------------

type Token =
  | { kind: 'num'; value: number }
  | { kind: 'ident'; value: string }
  | { kind: 'op'; value: '+' | '-' | '*' | '/' }
  | { kind: 'lparen' }
  | { kind: 'rparen' }
  | { kind: 'comma' }

export class FormulaError extends Error {}

function tokenize(input: string): Token[] {
  const tokens: Token[] = []
  let i = 0
  while (i < input.length) {
    const c = input[i]
    if (/\s/.test(c)) {
      i++
      continue
    }
    if (c === '(') {
      tokens.push({ kind: 'lparen' })
      i++
    } else if (c === ')') {
      tokens.push({ kind: 'rparen' })
      i++
    } else if (c === ',') {
      tokens.push({ kind: 'comma' })
      i++
    } else if (c === '+' || c === '-' || c === '*' || c === '/') {
      tokens.push({ kind: 'op', value: c })
      i++
    } else if (/[0-9.]/.test(c)) {
      let j = i
      while (j < input.length && /[0-9.]/.test(input[j])) j++
      const raw = input.slice(i, j)
      const value = Number(raw)
      if (Number.isNaN(value)) throw new FormulaError(`Invalid number: "${raw}"`)
      tokens.push({ kind: 'num', value })
      i = j
    } else if (/[A-Za-z_]/.test(c)) {
      let j = i
      while (j < input.length && /[A-Za-z0-9_]/.test(input[j])) j++
      tokens.push({ kind: 'ident', value: input.slice(i, j) })
      i = j
    } else {
      throw new FormulaError(`Unexpected character: "${c}"`)
    }
  }
  return tokens
}

// ---- Recursive-descent parser ---------------------------------------------

export function parseFormula(input: string): Ast {
  const tokens = tokenize(input)
  let pos = 0

  const peek = () => tokens[pos]
  const next = () => tokens[pos++]
  const expect = (kind: Token['kind']) => {
    const t = next()
    if (!t || t.kind !== kind) throw new FormulaError(`Expected ${kind}`)
    return t
  }

  function parseExpr(): Ast {
    let left = parseTerm()
    while (peek()?.kind === 'op' && (peek() as { value: string }).value.match(/[+-]/)) {
      const op = (next() as { value: '+' | '-' }).value
      left = { type: 'binary', op, left, right: parseTerm() }
    }
    return left
  }

  function parseTerm(): Ast {
    let left = parseFactor()
    while (peek()?.kind === 'op' && (peek() as { value: string }).value.match(/[*/]/)) {
      const op = (next() as { value: '*' | '/' }).value
      left = { type: 'binary', op, left, right: parseFactor() }
    }
    return left
  }

  function parseFactor(): Ast {
    const t = peek()
    if (!t) throw new FormulaError('Unexpected end of formula')
    if (t.kind === 'op' && t.value === '-') {
      next()
      return { type: 'unary', op: '-', operand: parseFactor() }
    }
    if (t.kind === 'num') {
      next()
      return { type: 'num', value: t.value }
    }
    if (t.kind === 'lparen') {
      next()
      const e = parseExpr()
      expect('rparen')
      return e
    }
    if (t.kind === 'ident') {
      next()
      if (peek()?.kind === 'lparen') {
        next()
        const args: Ast[] = []
        if (peek()?.kind !== 'rparen') {
          args.push(parseExpr())
          while (peek()?.kind === 'comma') {
            next()
            args.push(parseExpr())
          }
        }
        expect('rparen')
        return { type: 'call', name: t.value, args }
      }
      return { type: 'ident', name: t.value }
    }
    throw new FormulaError(`Unexpected token`)
  }

  const ast = parseExpr()
  if (pos < tokens.length) throw new FormulaError('Trailing tokens after formula')
  return ast
}

/** Collect bare identifiers (primitive references), excluding function names. */
export function referencedIdentifiers(ast: Ast): string[] {
  const out = new Set<string>()
  const walk = (n: Ast) => {
    switch (n.type) {
      case 'ident':
        out.add(n.name)
        break
      case 'call':
        n.args.forEach(walk)
        break
      case 'binary':
        walk(n.left)
        walk(n.right)
        break
      case 'unary':
        walk(n.operand)
        break
      case 'num':
        break
    }
  }
  walk(ast)
  return [...out]
}

/** Does the AST use a raw division operator anywhere? (safe_div must be used instead.) */
export function usesRawDivision(ast: Ast): boolean {
  switch (ast.type) {
    case 'binary':
      return ast.op === '/' || usesRawDivision(ast.left) || usesRawDivision(ast.right)
    case 'unary':
      return usesRawDivision(ast.operand)
    case 'call':
      return ast.args.some(usesRawDivision)
    default:
      return false
  }
}

function checkFunctions(ast: Ast, errors: string[]): void {
  if (ast.type === 'call') {
    const arity = ALLOWED_FUNCTIONS[ast.name]
    if (arity === undefined) {
      errors.push(`Unknown function "${ast.name}" — not in the allowed DSL.`)
    } else if (arity !== 'variadic' && ast.args.length !== arity) {
      errors.push(`"${ast.name}" expects ${arity} args, got ${ast.args.length}.`)
    }
    ast.args.forEach((a) => checkFunctions(a, errors))
  } else if (ast.type === 'binary') {
    checkFunctions(ast.left, errors)
    checkFunctions(ast.right, errors)
  } else if (ast.type === 'unary') {
    checkFunctions(ast.operand, errors)
  }
}

export interface ValidationContext {
  /** Primitive ids available for the selected (dimension family, window). */
  availablePrimitives: ReadonlySet<string>
  /** Raw column names that are labels / leak_risk and MUST NOT be referenced directly. */
  forbiddenRawColumns: ReadonlySet<string>
  /** Coverage per primitive id, for the aggregate coverage estimate. */
  coverageByPrimitive?: Record<string, number>
}

/**
 * Validate a formula string against the DSL rules (TRD §7.6). Never throws for
 * user-input errors — returns a structured result the UI panel renders.
 */
export function validateFormula(input: string, ctx: ValidationContext): FormulaValidation {
  const errors: string[] = []
  let ast: Ast
  try {
    ast = parseFormula(input)
  } catch (e) {
    return {
      ok: false,
      typeCheck: 'fail',
      divisionSafety: 'fail',
      pointInTime: 'fail',
      primitiveAvailability: 'fail',
      coverageEstimate: 0,
      errors: [e instanceof FormulaError ? e.message : 'Parse error'],
    }
  }

  checkFunctions(ast, errors)
  const typeCheck: 'pass' | 'fail' = errors.length === 0 ? 'pass' : 'fail'

  const divisionSafety: 'pass' | 'fail' = usesRawDivision(ast) ? 'fail' : 'pass'
  if (divisionSafety === 'fail') {
    errors.push('Raw division "/" is not allowed — use safe_div(a, b).')
  }

  const idents = referencedIdentifiers(ast)

  // Point-in-time / no-future-label: a raw label/leak_risk column referenced directly.
  const leaks = idents.filter((id) => ctx.forbiddenRawColumns.has(id))
  const pointInTime: 'pass' | 'fail' = leaks.length === 0 ? 'pass' : 'fail'
  for (const l of leaks) {
    errors.push(
      `"${l}" is a label / leak_risk field and cannot be a direct input. ` +
        `Use a trailing-window aggregate primitive instead (schema §6).`,
    )
  }

  // Primitive availability: every bare identifier must be a known primitive.
  const missing = idents.filter((id) => !ctx.availablePrimitives.has(id) && !ctx.forbiddenRawColumns.has(id))
  const primitiveAvailability: 'pass' | 'fail' = missing.length === 0 ? 'pass' : 'fail'
  for (const m of missing) {
    errors.push(`Primitive "${m}" is not available for this dimension family / window.`)
  }

  // Coverage estimate: min coverage across referenced primitives (weakest link).
  const covs = idents
    .map((id) => ctx.coverageByPrimitive?.[id])
    .filter((v): v is number => typeof v === 'number')
  const coverageEstimate = covs.length ? Math.min(...covs) : 1

  const ok =
    typeCheck === 'pass' &&
    divisionSafety === 'pass' &&
    pointInTime === 'pass' &&
    primitiveAvailability === 'pass'

  return {
    ok,
    typeCheck,
    divisionSafety,
    pointInTime,
    primitiveAvailability,
    coverageEstimate,
    errors,
    outputType: ok ? 'numeric' : undefined,
  }
}
