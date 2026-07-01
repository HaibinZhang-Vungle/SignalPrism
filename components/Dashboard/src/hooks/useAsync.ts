import { useEffect, useState } from 'react'

/** Minimal async-load hook for the fixture data source. */
export function useAsync<T>(fn: () => Promise<T>, deps: unknown[] = []): T | undefined {
  const [value, setValue] = useState<T | undefined>(undefined)
  useEffect(() => {
    let alive = true
    fn().then((v) => {
      if (alive) setValue(v)
    })
    return () => {
      alive = false
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps)
  return value
}
