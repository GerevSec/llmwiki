// Standalone test for the mermaid preprocessor. Runs with `node --test`.
// Inlines the same regex as MermaidBlock.tsx so the test doesn't need a TS toolchain.

import { test } from 'node:test'
import assert from 'node:assert/strict'

function preprocessMermaidChart(chart) {
  return chart.replace(
    /(\b\w+)\[(\/[^\]"\n]+?)(?<!\/)\]/g,
    (_match, id, body) => `${id}["${body}"]`,
  )
}

test('quotes bracketed labels that start with / (the home-page bug)', () => {
  const input = 'OV --> ARCH[/wiki/concepts/architecture.md]'
  const out = preprocessMermaidChart(input)
  assert.equal(out, 'OV --> ARCH["/wiki/concepts/architecture.md"]')
})

test('does not touch valid parallelogram shape [/text/]', () => {
  const input = 'A[/valid parallelogram/]'
  assert.equal(preprocessMermaidChart(input), input)
})

test('does not touch normal labels without leading slash', () => {
  const input = 'OV[Overview — this page]'
  assert.equal(preprocessMermaidChart(input), input)
})

test('handles the full Be overview navigation block', () => {
  const input = [
    'graph TD',
    '    OV[Overview — this page]',
    '    OV --> ARCH[/wiki/concepts/architecture.md]',
    '    OV --> LA[/wiki/concepts/life-agent.md]',
    '    OV --> BEDEV[/wiki/entities/be-device.md]',
  ].join('\n')
  const out = preprocessMermaidChart(input)
  assert.ok(out.includes('ARCH["/wiki/concepts/architecture.md"]'))
  assert.ok(out.includes('LA["/wiki/concepts/life-agent.md"]'))
  assert.ok(out.includes('BEDEV["/wiki/entities/be-device.md"]'))
  // Non-slash labels untouched
  assert.ok(out.includes('OV[Overview — this page]'))
})

test('multiple slash labels on the same line all get quoted', () => {
  const input = 'A[/foo] --> B[/bar]'
  assert.equal(preprocessMermaidChart(input), 'A["/foo"] --> B["/bar"]')
})
