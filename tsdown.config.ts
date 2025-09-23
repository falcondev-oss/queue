import { defineConfig } from 'tsdown'

export default defineConfig({
  clean: true,
  outDir: 'dist',
  entry: ['src/index.ts'],
  format: ['esm', 'cjs'],
  platform: 'node',
  dts: true,
})
