import nextra from 'nextra'

const withNextra = nextra({
  defaultShowCopyCode: true,
  search: {
    codeblocks: false
  }
})

// Static export + base path are opt-in via env so local `next dev`/`next build`
// stay at the root and use the server runtime. The GitHub Pages workflow sets
// EXPORT=true and NEXT_PUBLIC_BASE_PATH=/<repo> (a project site lives under a subpath).
const isExport = process.env.EXPORT === 'true'
const basePath = process.env.NEXT_PUBLIC_BASE_PATH || ''

/** @type {import('next').NextConfig} */
const config = {
  reactStrictMode: true,
  experimental: {
    optimizePackageImports: ['nextra-theme-docs']
  }
}

if (isExport) {
  config.output = 'export'
  config.images = { unoptimized: true }
  config.trailingSlash = true
}

if (basePath) {
  config.basePath = basePath
}

export default withNextra(config)
