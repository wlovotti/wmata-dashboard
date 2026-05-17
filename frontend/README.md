# React + Vite

This template provides a minimal setup to get React working in Vite with HMR and some ESLint rules.

Currently, two official plugins are available:

- [@vitejs/plugin-react](https://github.com/vitejs/vite-plugin-react/blob/main/packages/plugin-react) uses [Babel](https://babeljs.io/) (or [oxc](https://oxc.rs) when used in [rolldown-vite](https://vite.dev/guide/rolldown)) for Fast Refresh
- [@vitejs/plugin-react-swc](https://github.com/vitejs/vite-plugin-react/blob/main/packages/plugin-react-swc) uses [SWC](https://swc.rs/) for Fast Refresh

## React Compiler

The React Compiler is not enabled on this template because of its impact on dev & build performances. To add it, see [this documentation](https://react.dev/learn/react-compiler/installation).

## Expanding the ESLint configuration

If you are developing a production application, we recommend using TypeScript with type-aware lint rules enabled. Check out the [TS template](https://github.com/vitejs/vite/tree/main/packages/create-vite/template-react-ts) for information on how to integrate TypeScript and [`typescript-eslint`](https://typescript-eslint.io) in your project.

## Running tests

### Unit tests (Vitest)

```bash
npm test            # single run — same as CI
npm run test:watch  # watch mode for local development
```

Unit tests live in `tests/unit/` and cover the shared primitives:
`DeltaIndicator`, `computeWindowDelta`, `TargetIndicator`, `Sparkline`
(from `RouteTrend.jsx`), `formatters.js`, `spectrumBar.js`,
`frequencyClass.js`, and `useMultiFetch.js`.

These are **characterization tests** — they assert what the current code
does, not aspirational behavior. When you change a primitive, update the
test to match rather than "fixing" the test silently.

### Visual regression tests (Playwright)

```bash
npx playwright install chromium          # first-time browser install
npx playwright test                      # run all specs (must pass unit first)
npx playwright test --update-snapshots   # regenerate baselines after UI changes
npx playwright test --ui                 # interactive UI mode for debugging
```

Playwright specs live in `tests/e2e/` and cover three pages: Overview (`/`),
RouteList (`/routes`), and RouteDetail for route D72 (`/route/D72`). All
`/api/**` calls are intercepted by `page.route()` and served from committed
JSON fixtures in `tests/fixtures/` — no backend is required.

**Baseline snapshots are platform-specific.** Playwright stores one PNG per
platform (`*-chromium-linux.png`, `*-chromium-darwin.png`). CI runs on Linux
and validates against the `*-linux.png` baselines; macOS devs see `*-darwin.png`
locally. Both are committed.

When you change UI that affects a baselined page, regenerate **both** sets:

```bash
# macOS baselines (run locally on your Mac):
npx playwright test --update-snapshots

# Linux baselines (run via Docker so CI passes):
docker run --rm -v "$(pwd):/work" -v /work/node_modules -w /work \
  mcr.microsoft.com/playwright:v1.60.0-noble \
  bash -c "npm ci --silent && npx playwright test --update-snapshots"
```

If you only regenerate the macOS set, CI will fail on the stale Linux baseline.
If you skip Docker and only regenerate Linux via a CI-update PR, your local
runs will diff against an outdated darwin snapshot.
