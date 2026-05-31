import type { ComponentType, ReactNode } from 'react'
import { generateStaticParamsFor, importPage } from 'nextra/pages'
import { useMDXComponents as getMDXComponents } from '../../mdx-components'

export const generateStaticParams = generateStaticParamsFor('mdxPath')

type PageProps = {
  params: Promise<{ mdxPath: string[] }>
}

export async function generateMetadata(props: PageProps) {
  const params = await props.params
  const { metadata } = await importPage(params.mdxPath)
  return metadata
}

// `wrapper` is typed as optional on MDXComponents; it is always provided by the
// docs theme, so narrow it to a concrete component type for JSX use.
const Wrapper = getMDXComponents().wrapper as ComponentType<{
  toc: unknown
  metadata: unknown
  children: ReactNode
}>

export default async function Page(props: PageProps) {
  const params = await props.params
  const result = await importPage(params.mdxPath)
  const { default: MDXContent, toc, metadata } = result
  return (
    <Wrapper toc={toc} metadata={metadata}>
      <MDXContent {...props} params={params} />
    </Wrapper>
  )
}
