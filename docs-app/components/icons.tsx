import type { ReactNode } from 'react'

const base = {
  width: 20,
  height: 20,
  viewBox: '0 0 24 24',
  fill: 'none',
  stroke: 'currentColor',
  strokeWidth: 1.7,
  strokeLinecap: 'round' as const,
  strokeLinejoin: 'round' as const
}

function Svg({ children }: { children: ReactNode }) {
  return <svg {...base}>{children}</svg>
}

/** stacked records / append-only log */
export const IconLog = () => (
  <Svg>
    <rect x="3" y="4" width="18" height="4" rx="1" />
    <rect x="3" y="10" width="18" height="4" rx="1" />
    <rect x="3" y="16" width="11" height="4" rx="1" />
  </Svg>
)

/** explicit loss / gap flag */
export const IconFlag = () => (
  <Svg>
    <path d="M5 21V4" />
    <path d="M5 4h11l-2 3 2 3H5" />
  </Svg>
)

/** routers / fan-out */
export const IconRoute = () => (
  <Svg>
    <circle cx="5" cy="6" r="2" />
    <circle cx="19" cy="12" r="2" />
    <circle cx="5" cy="18" r="2" />
    <path d="M7 6h6a3 3 0 0 1 3 3v0a3 3 0 0 0 1 2M7 18h6a3 3 0 0 0 3-3v0a3 3 0 0 1 1-2" />
  </Svg>
)

/** SSE / live delivery */
export const IconBolt = () => (
  <Svg>
    <path d="M13 2 4 14h7l-1 8 9-12h-7l1-8Z" />
  </Svg>
)

/** lease queue / visibility timeout */
export const IconClock = () => (
  <Svg>
    <circle cx="12" cy="12" r="9" />
    <path d="M12 7v5l3 2" />
  </Svg>
)

/** single binary */
export const IconCube = () => (
  <Svg>
    <path d="M12 2 3 7v10l9 5 9-5V7l-9-5Z" />
    <path d="M3 7l9 5 9-5M12 12v10" />
  </Svg>
)

/** durability / disk */
export const IconDisk = () => (
  <Svg>
    <path d="M4 5h16v14H4z" />
    <path d="M8 5v6h8V5M9 15h.01" />
  </Svg>
)

/** shield / security */
export const IconShield = () => (
  <Svg>
    <path d="M12 3l8 3v6c0 5-3.5 8-8 9-4.5-1-8-4-8-9V6l8-3Z" />
    <path d="M9 12l2 2 4-4" />
  </Svg>
)
