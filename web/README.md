# SentinelFlow dashboard

A React + TypeScript dashboard over the incidents API: live incident list,
incident detail with the alert timeline and remediation trail, and the controls
to acknowledge, resolve, and approve or reject a gated remediation step.

```bash
npm install
npm run dev        # http://localhost:3000, proxying /v1 to localhost:8084
```

`npm run dev` proxies `/v1` to `http://localhost:8084` (override with
`INCIDENTS_API_URL`), so run `make up` in the repository root first. In the
Compose stack the dashboard is served by nginx at
<http://localhost:3000> with the same proxy, so the app's fetch paths are
identical in both modes.

| Script | Does |
|---|---|
| `npm run dev` | Vite dev server with API proxy |
| `npm run typecheck` | `tsc --noEmit` |
| `npm run build` | Typecheck, then production bundle into `dist/` |
| `npm run preview` | Serve the built bundle locally |

## Deliberate choices

**No UI framework, no state library, no router.** The app is two panes and four
components. React and React DOM are the only runtime dependencies; everything
else — layout, theming, the polling hook, the typed API client — is a few dozen
lines each. A component library would be a larger dependency than the
application, and a router would exist to manage exactly one piece of state.

**Polling, not websockets.** The backend exposes a REST read API and no push
channel, so a socket would be a fiction layered over the same requests. The list
and detail views refresh every 5 seconds, which is honest and entirely adequate
for an incident list. If sub-second liveness is ever wanted, that is a backend
change (server-sent events) and `usePolling` is the seam it lands in.

**Same-origin by design.** nginx (production) and Vite (dev) both proxy `/v1`, so
the browser never makes a cross-origin request and the Go API carries no CORS
middleware.

**Server error messages are surfaced verbatim.** The API distinguishes 404, 409
(illegal transition, or nothing awaiting a decision) and 503 (remediation not
configured), and each tells the user something different about what to do next.
`ApiError` keeps the status and code so the UI can say "this incident is already
resolved" instead of "request failed".

**Types are hand-written**, mirroring the Go JSON in `src/types.ts`. The surface
is small and stable; generating them would be more machinery than it saves. If it
grows, generate from an OpenAPI document rather than letting them drift.

## Not done

- **No authentication.** The API has none either; the dashboard sends `actor=dashboard`
  when approving. Real deployments take the actor from an authenticated session.
- **No tests.** The Go side is thoroughly tested; the dashboard's logic is thin
  enough that a testing stack would be more setup than assertion. The build does
  run `tsc --noEmit` in CI, so type regressions are caught.
- **No charts or dashboards over the event stream.** The read API supports it
  (`GET /v1/events`); the visualisation is future work.
