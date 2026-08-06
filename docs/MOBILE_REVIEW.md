# Mobile app review — code, architecture, UI, and the TV gap

**Status:** P0 findings reconciled; remaining punch list is open ·
**Reviewed at:** `2ae83b0` (main) · **Written:** 2026-08-02 ·
**P0 reconciled:** 2026-08-06 · **Reviewer:** Claude (independent code review)

Companion to [MOBILE.md](MOBILE.md) (how to build and run the app) — this is
an outside audit of the app in [`mobile/`](../mobile/): code quality,
architecture, UI/UX, performance, release readiness, and what "Google TV and
Apple TV" would actually take. Line numbers are as of `2ae83b0` and will
drift; function names won't.

How this review was verified: the full TypeScript source was read (~3.6k
lines), `npm run typecheck` passes clean, all 41 jest tests pass, and every
client-side wire assumption was cross-checked against the server handlers in
[`crates/nzbd-api/src/lib.rs`](../crates/nzbd-api/src/lib.rs) (SSE event
names and heartbeat timing, query parameters, DTO field names, auth and
role headers). Claims below that say "verified" mean checked against both
sides, not assumed. At the P0 reconciliation point, all 48 Jest tests pass.

**Scope note.** The repo contains one Expo SDK 57 / React Native 0.86 app
targeting iOS, iPadOS, and Android phones/tablets. There is no tvOS target,
no Android TV (leanback) declaration, and no TV-capable react-native fork in
`package.json` — as committed, this codebase cannot produce an Apple TV app
and produces a Google TV app only in the sideload-and-squint sense. §8
treats that as the gap to close rather than a defect: nothing in the repo
claims TV support today ([MOBILE.md](MOBILE.md) says "iPhone, iPad, and
Android").

---

## 1. Verdict

The shape of this app is right. Layering is clean and one-directional,
TypeScript is strict with faithful server DTO mirrors, the SSE lifecycle is
handled more correctly than most production dashboards manage, and the tests
sit at exactly the fragile joints (chunk-split SSE parsing, URL
normalization, discovery host selection). For a v1 remote, it is well built.

The original review's highest risks were concentrated in four places. The two
P0 defects are now closed; their sections retain the original finding and add
the implemented resolution:

| # | Risk | Where | Severity | Status |
|---|---|---|---|---|
| §4.1 | No list virtualization + whole-tree re-render on every SSE tick | all three tabs | High (perf/battery) | Open |
| §7.1 | Release builds used the committed debug keystore | `android/app/build.gradle` | High (release blocker) | Complete 2026-08-06 |
| §3.1 | Basic-auth header crashed on non-Latin-1 passwords | `src/api/client.ts` | Medium (correctness) | Complete 2026-08-06 |
| §8 | Stated platform ambition (TV) has no target in the repo | everywhere | Strategic | Open |

Everything else is listed by area below, each with the failure it causes and
a concrete fix. §11 is the prioritized punch list.

---

## 2. What's solid — keep doing this

The architecture is a textbook thin-client split, and dependencies point one
way only:

```
App.tsx ──▶ screens/ ──▶ hooks/useNzbd ──▶ api/client ──▶ expo/fetch
                │                │              │
                │                └─▶ api/sse    └─▶ api/types (DTO mirror)
                ├─▶ discovery/  (DNS-SD → baseUrl, isolated)
                ├─▶ storage/    (SecureStore, isolated)
                └─▶ theme.ts    (context, light/dark/system)
```

**The SSE lifecycle is genuinely correct** (verified against
`sse_events` in `lib.rs`): the client resumes with `Last-Event-ID`, treats
`reset` and `lagged` as "poll to reconcile" exactly as the server contract
intends, backs off exponentially (1s → 15s cap), and detects staleness with
a 7-second threshold that safely clears the server's 5-second `hb`
heartbeat (`HEARTBEAT_AFTER`). The REST poll only fires when the stream is
actually stale. Most clients get at least one of these wrong.

**The API client is honest about identity and role.** `X-Nzbd-Client:
nzbd-mobile/1.0` plus `X-Nzbd-Role: operator` — verified server-side in
`consumer_name()`: the operator role exempts the app's history reads from
`picked_up_by` attribution, so browsing History on your phone doesn't
impersonate an *arr pickup. That is a subtle contract, and both sides
honor it.

**Types don't lie.** `src/api/types.ts` mirrors the server DTOs
field-for-field, including the awkward Rust enum shape of `JobStatus`
(`'downloading' | { post: { stage } }`), and there's a test pinning both
shapes. `tsconfig` is `strict: true` and the code has no `any` escapes.

**Failure states exist everywhere.** Every screen has loading, empty, and
error states with retry affordances; errors are sentence-case, specific,
and name the fix ("Docker installs need the host-network discovery
companion"). Accessibility is above average for a v1: roles, labels,
`accessibilityState`, live regions on error banners, and a real
`progressbar` value on storage meters.

**Security posture is deliberate and documented.** Credentials live in
Keychain/Keystore via SecureStore, there is no "ignore certificate errors"
switch, and [MOBILE.md](MOBILE.md) states the HTTP-on-LAN trade honestly.

---

## 3. Correctness findings

### 3.1 `btoa` throws on non-Latin-1 passwords

`client.ts:168` builds Basic auth with `btoa(`${username}:${password}`)`.
`btoa` rejects any code point above U+00FF (`InvalidCharacterError`), so a
password containing anything outside Latin-1 — an em-dash, a curly quote
from iOS smart punctuation, any non-ASCII character — throws on **every
request**, presenting as an unexplained connection failure rather than a
credentials problem. iOS autocorrect makes smart-quote injection into
passwords a realistic path, not a theoretical one.

Fix: encode UTF-8 first, per RFC 7617's `charset="UTF-8"` reality:

```ts
const bytes = new TextEncoder().encode(`${user}:${pass}`);
const b64 = btoa(String.fromCharCode(...bytes));
```

(`TextEncoder` is already in scope via Expo's winter runtime — the SSE
reader uses `TextDecoder`.) One test with a non-ASCII password pins it.

**Resolved 2026-08-06:** the client now encodes the complete credential pair
as UTF-8 before Base64 encoding. A wire-level unit test pins an exact header
containing non-ASCII username and password characters, and a second test keeps
Bearer-token precedence explicit.

### 3.2 History "load more" races the moving list

`HistoryView.loadMore()` pages with `offset = page.entries.length` against
a newest-first list. Any job that completes between page fetches shifts
every offset by one: the next page then repeats the last entry of the
previous page (duplicate — and a duplicate React key
`` `${entry.job}:${entry.completed_at_unix}` ``, so a render warning and a
ghost row) or skips one silently. On an active server this is routine, not
rare.

Cheapest fix: de-duplicate by `entry.job` when concatenating pages. Proper
fix: page by sequence — the server already exposes `seq` per entry and a
`since_seq` cursor (see `HistoryQuery` in `lib.rs`); a `before_seq`
mirror would make backward paging exact. The client-side dedupe is five
lines and removes the visible symptom today.

### 3.3 A REST refresh can overwrite a newer SSE tick

`useNzbd.refresh()` writes `setSnapshot(next)` unconditionally. Sequence:
mutation → `refresh()` issued → tick arrives with the post-mutation state →
the slower REST response lands and rewinds the UI to pre-tick state. It
self-heals on the next changed tick, but during an idle-ish queue the
server suppresses duplicate ticks, so the stale rewind can sit on screen
until the next real change. Guard: record `Date.now()` when the refresh
starts and skip `setSnapshot` if a tick was accepted after that instant
(`lastFrameAt.current` already holds the timestamp you need).

### 3.4 `normalizeServerUrl` forces `:6789` onto explicit-`https` origins

`format.ts` appends the nzbd default port whenever none is typed — the
right call for bare hosts (`nuc3` → `http://nuc3:6789`), but it also turns
`https://nzbd.example.com` into `https://nzbd.example.com:6789`. The
common reason a user types an `https://` URL is a reverse proxy on 443,
which this rewrite breaks; the workaround (explicitly typing `:443`) is
not discoverable. The test suite pins the current behavior
(`format-test.ts:43`), so this is a decision to revisit, not an accident:
apply the 6789 default only when the user typed no scheme (or scheme
`http`), and let an explicit `https://host` keep its own default port.

### 3.5 The 12-second timeout applies to NZB uploads

Every request through `NzbdClient.request()` gets a 12s abort, including
`addNzb`'s body upload. Large NZBs (tens of MB for big sets) over weak
Wi-Fi or a VPN can exceed that, aborting mid-upload with "did not answer
within 12 seconds" — misleading, since the server was answering fine.
Give mutations with bodies their own budget (60s, or no timeout with the
existing AbortController wired to a Cancel button in the add sheet).

### 3.6 `formatDuration` can render "1h 60m" and "60m"

`Math.ceil` on the minutes remainder: 7,170–7,199s renders `1h 60m`;
3,571–3,599s renders `60m` instead of `1h`. Carry the rounded minutes into
hours before formatting. One `test.each` row each.

---

## 4. Performance

### 4.1 No virtualization, and every tick re-renders the entire tree

This is the finding that will bite first as queues grow. All three tabs
render `array.map(...)` inside a plain `ScrollView` — every row is mounted,
measured, and kept live regardless of viewport. During active downloads the
server emits a changed `tick` roughly every second (verified: 1 Hz loop,
duplicate-suppressed), and each tick replaces `snapshot`, which:

- recomputes `sectionQueueJobs` (fine — it's O(n) and memoized on `jobs`),
- re-renders **every** `JobCard`, `StorageVolume`, and `SummaryMetric` —
  nothing is wrapped in `React.memo`, and every callback passed to
  `JobCard` is a fresh arrow function per render, so memoization wouldn't
  currently help anyway,
- re-renders whether or not the changed field is visible.

At 10 jobs nobody notices. At a 200-job queue plus 200 history rows plus
the logs tab, this is continuous layout work at 1 Hz on a phone SoC —
dropped frames while scrolling and measurable battery cost.

Fix, in order of leverage:

1. `SectionList` (or FlashList) for the queue with `keyExtractor={job.id}`,
   sections from `sectionQueueJobs` — it already produces section-shaped
   data; `FlatList` for history and logs.
2. Extract `JobCard` handlers so rows receive stable props (`onAction(id,
   action)` at the list level, row calls it with its own id), then
   `React.memo` the row. A job row's props then only change when *that
   job's* summary changes.
3. Keep the overview dock outside the list (it already is) so per-second
   rate changes touch only the dock and the rows whose bytes moved.

### 4.2 The logs tab re-downloads up to 1,000 rows every 4 seconds

`LogsView` polls `getLogs(includeFiles)` — `limit=1000` — on a 4s interval
and re-renders the full reversed list each time. Two server features make
this unnecessary (both verified in `lib.rs`):

- `GET /api/v1/logs?after=<id>` returns only newer entries — an
  incremental poll is a cursor away (`LogEntry.id` is already in the app's
  types).
- The already-open SSE stream batches new log lines at 1 Hz as `log`
  events (`{ entries, dropped }`). `useNzbd.acceptFrame` currently ignores
  them. Routing them to subscribers would make the logs tab live *for
  free* on the existing connection, and the REST endpoint becomes backfill
  only — exactly how the web UI treats it.

Either is strictly better than the current loop; the SSE route also drops
the second polling timer entirely.

### 4.3 One global `busyKey` serializes the whole UI

Every control disables when *any* mutation is in flight (`busy={busyKey
!== null}`), including all other jobs' buttons and the header pause. On a
slow LAN, reordering five jobs is five sequential rounds of
disable-everything. The key is already namespaced (`job:12:pause`) — scope
the disable to the acting job and the specific control, and let the queue
pause button stay independent. Also: each mutation triggers a full
`refresh()`, duplicating the tick that arrives within a second anyway;
with §3.3's guard in place, the explicit refresh can become a fallback
(only when the stream isn't live) instead of a constant.

### 4.4 Tab switches unmount and refetch

`activeSection` conditionally renders `HistoryView`/`LogsView`, so each
visit loses scroll position, expanded cards, and the file-details toggle,
and re-fetches history from offset 0. Keep all three mounted and toggle
visibility (`display: 'none'`), or lift the fetched pages into a parent
cache. On TV (§8) this matters more — focus memory across tab switches is
expected there.

### 4.5 The stream keeps its socket while backgrounded

`AppState` is only used to refresh on foreground. On Android the SSE
socket and the 5s/4s timers can keep running in the background longer than
iOS allows, costing battery for a screen nobody is watching. Tear down the
stream and timers on `background`/`inactive` and rebuild on `active` — the
`Last-Event-ID` resume path you already implemented makes this cheap and
gapless.

### 4.6 Small things

- `ActionButton` calls `makeStyles(theme)` on every render with no
  `useMemo` — inconsistent with every other component; hoist or memoize.
- The document picker copies NZBs to cache (`copyToCacheDirectory: true`)
  and never deletes them after upload; harmless until someone queues a
  season of 50 MB NZBs. Delete the cache copy after a successful add.

---

## 5. UX and product gaps

### 5.1 There is no way to hand the app an NZB (the missing front door)

The single most common acquisition flow — tap an `.nzb` link in Safari, or
share one from another app — dead-ends: the app declares a `nzbd://` URL
scheme in three places (`app.json`, `AndroidManifest.xml`,
`Info.plist`) but contains **no `Linking` handler at all**, no iOS
document-type declaration for `.nzb`, and no Android `VIEW`/`SEND` intent
filters for NZB files. The only intake is pull (the picker inside the
modal). For an NZB tool, being a share-sheet target is the difference
between "remote control" and "the way you use nzbd from your phone."

Concrete path:

- iOS: `CFBundleDocumentTypes` + `UTExportedTypeDeclarations` for
  `application/x-nzb` / `.nzb`, handle the open-in URL, prefill the add
  sheet.
- Android: `intent-filter` for `ACTION_VIEW`/`ACTION_SEND` with the NZB
  mime/extension, same prefill.
- Bonus: the server's `POST /api/v1/jobs?url=` fetch-side add (verified
  present in `AddJobQuery`) means a shared *link* needs no download on
  the phone at all — and it is the add path a future TV target will need
  anyway (§8.2).

### 5.2 Auth failures retry forever with a generic banner

A 401 from the stream or a poll produces "Event stream returned HTTP 401."
and an infinite reconnect loop. `ApiError` already carries `status`; the
stream loop discards it. On 401/403: stop the backoff, and show a specific
state — "nzbd rejected these credentials" with an **Edit connection**
button. Wrong-password is the most common first-run failure; it deserves
the best error, not the most generic one.

### 5.3 The guard banner is blind to the failures you actually had

`GuardBanner` surfaces `disk_low`, `quota_reached`, and blocked servers.
The status DTO also carries `enospc_observed` / `enospc_where` and
`health_abort` — and the July par-repair incident was precisely an ENOSPC
state that no guard surfaced. The phone in your pocket is the best place
for "the volume filled up": add ENOSPC (with `enospc_where`) and
health-abort lines to the banner, and show `speed_limit_bps` somewhere
when set — a capped queue currently looks identical to a slow one.

### 5.4 History is read-only — worth revisiting one notch

[MOBILE.md](MOBILE.md) documents this as a deliberate limit, and the
restraint is respectable. But `requeue` and `restore` (both already in the
native API: `POST /api/v1/history/{id}/actions/…`) are exactly the actions
you want *away from a desk*: a failed job you'd like to retry now that a
provider unblocked. Suggest: keep destructive actions (delete,
delete-files, hide) web-only, add the two non-destructive verbs to mobile.
If the limit stands, the review's ask is one sentence in MOBILE.md saying
*why* (parity risk? accidental-tap risk?), so the next reader doesn't
re-litigate it.

### 5.5 The server's identity is invisible after connect

Version, uptime, and session-downloaded bytes are fetched on every status
tick and shown nowhere (version appears only in the connect-test toast).
One quiet row — `nzbd 0.2.0 · up 6d · 214 GiB this session` — under the
providers panel answers "did my upgrade take?" without ssh.

### 5.6 "updated Xs ago" freezes on an idle queue

`formatAge` renders from `lastUpdated`, but an idle queue emits only `hb`
frames, which change no state — so "updated 3s ago" can sit for minutes.
Either tick a 1-second interval while the screen is visible, or (simpler)
show nothing when `connectionState === 'live'` — the green dot already
says it.

---

## 6. UI design and brand

### 6.1 The overview dock's type scale is below the readable floor

The dock uses `fontSize: 7` (metric labels, storage paths, "critical"
note), `8` (storage labels/capacity), and `9` (block titles, storage
percent). Apple's floor for legible UI text is ~11pt; 7pt is decorative at
phone distance and vanishes at TV distance. Nothing in the app sets
`allowFontScaling` policy either, so Dynamic Type at accessibility sizes
will scale these tiles into their fixed-height containers and clip.
Adopt a ramp (10/11/13/15/17/20/23 covers every current use), stop below
10 for nothing, and set `maxFontSizeMultiplier` where tiles genuinely
cannot grow.

### 6.2 Three palettes, no source of truth

Three surfaces, three unrelated color systems (none of them the brand kit):

| Token | Web UI (`ui/index.html`) | Mobile (`theme.ts`) | noirr kit (`tokens.css`) |
|---|---|---|---|
| bg (dark) | `#0f1013` | `#0B1219` (blue-tinted) | `#0a0a0c` |
| panel/surface | `#17181c` | `#121C25` | `#101014` |
| accent | `#4a8fe0` (blue) | `#55AFFF` (blue) | `#e5484d` (crimson) |
| ok / warn / err | `#3fae3f` / `#d8a63a` / `#e05a52` | `#4ED29D` / `#F3B95F` / `#FF7D86` | `#5fb582` / `#d9a05b` / `#ff7a66` |
| type | system sans | system sans | JetBrains Mono + Inter |

If noirr (`~/Downloads/kit 2`) is the direction — and it reads like a
considered one — neither shipped surface follows it yet, and mobile and web
have also drifted from *each other* (compare the dark backgrounds above).
The fix is structural, not cosmetic: make `tokens.css` the canonical
semantic set and derive both surfaces from it. The mapping is nearly 1:1
already:

```
noirr bg → theme.background     noirr ink  → theme.text
noirr surface → theme.panel     noirr dim  → theme.textMuted
noirr raised → theme.panelAlt   noirr line → theme.border
noirr accent → theme.accent     ok/warn/err → success/warning/danger
```

A 30-line script (or a jest test asserting the hex values match) keeps
`theme.ts` honest against `tokens.css`, the same way this repo already
uses tests to keep docs honest. The mobile app would also need the brand
faces bundled (`expo-font`: JetBrains Mono for job names, sizes, rates —
"mono is the voice of the system" — Inter for prose); `fontVariant:
['tabular-nums']` is already used in the right places, which makes the
switch to a mono face for technical strings a natural completion.

### 6.3 The stage-accent rainbow fights the brand's core principle

`QUEUE_STAGE_ACCENTS` hardcodes eight hues (purple renaming, teal
verifying, pink extracting…). It's cheerful, but noirr's stated principle
is "UI monochrome, posters/artwork carry the color," and eight
simultaneous accent hues is the opposite. It also duplicates meaning: the
section header already names the stage. Under a noirr pass, stages would
collapse to: accent for active work, `warn` for waiting states, `err` for
repair/failure — three semantic tones instead of eight decorative ones.
(Independent of brand: the eight hex values are hardcoded outside
`theme.ts`, so they don't adapt if the palette ever changes — move them
into the theme regardless.)

### 6.4 Hardcoded whites

`'#FFFFFF'` appears as the brand-mark glyph color, selected-priority-chip
text, and `ActionButton`'s primary label/spinner. All are really one
semantic token — "text on accent" — and will break the day the accent
lightens (noirr matinee's red-ink accent at `#c2343a` keeps white
workable; a future giallo amber would not). Add `onAccent` to `Theme`.

### 6.5 Accessibility details

Good bones (see §2). Two touch-ups: `accessibilityRole="header"` sits on
a `View` in the queue group heading — screen readers want it on the
`Text`; and the connection dot + status ("live · http://nuc3:6789") is
the only always-visible connection indicator but has no accessibility
label tying dot color to meaning — add
`accessibilityLabel={`connection ${connectionLabel(state)}`}` on the row.

---

## 7. Native config and release readiness

### 7.1 Release builds used the committed debug keystore

`android/app/build.gradle` wires `release { signingConfig
signingConfigs.debug }`, and `debug.keystore` (password `android`, the
publicly known RN default) is committed. Consequences, in order: Play
Store will not accept it; any APK you hand out can be impersonated and
"upgraded" by anyone else's build signed with the same universal key; and
once real users install debug-signed builds, switching to a real key
forces uninstall/reinstall (signature mismatch), losing SecureStore
credentials. Before any distribution beyond your own devices: generate an
upload keystore, reference it via `gradle.properties`-outside-VCS or env,
and keep the debug config for debug only. The gradle comment already
warns about this; the review's job is to say it *now blocks* anything
beyond sideloading to your own hardware.

**Resolved 2026-08-06:** debug builds retain the development keystore, but a
release artifact can only be assembled, bundled, installed, published, or
signed when all four external upload-key settings are present. Partial
configuration and missing release configuration both fail with named errors;
the secrets and keystore remain outside version control. CI proves the
secretless release path fails closed, and [MOBILE.md](MOBILE.md) documents the
signed-release procedure.

### 7.2 ATS is fully disabled; cleartext is app-wide

`NSAllowsArbitraryLoads: true` plus Android `usesCleartextTraffic: true`
permits HTTP to *any* host, not just the LAN. [MOBILE.md](MOBILE.md)
documents why (user-entered private IPs can't be enumerated at build
time) and the reasoning is sound — but two refinements are available
without giving up the use case:

- App Store review asks for justification of `NSAllowsArbitraryLoads`;
  have the MOBILE.md paragraph ready as the answer, or scope it: keep
  `NSAllowsLocalNetworking` and accept `http://` only for
  RFC 1918 / `.local` / link-local hosts in `normalizeServerUrl`,
  warning (not blocking) on public-address HTTP. That turns a global
  waiver into a policy the app itself enforces.
- Same policy on Android via a `networkSecurityConfig` is possible but
  fights user-entered hostnames; the in-app warning is the portable half.

### 7.3 Leftover dev permissions in the main manifest

`SYSTEM_ALERT_WINDOW` (draw over other apps — flagged in Play review and
scary in the permission list) and `VIBRATE` sit in the **main** manifest;
the app uses neither. The overlay permission belongs in the debug
manifest (dev-client error overlay) if anywhere. Also present:
`READ/WRITE_EXTERNAL_STORAGE` (maxSdk 32) — standard Expo scaffolding the
document picker doesn't need on current targets; prune at the next
prebuild and diff the merged manifest (`gradlew :app:processReleaseManifest`).

### 7.4 Version numbers live in three places; mobile checks live in no CI

`1.0.0`/build `1` is hardcoded in `app.json`, `Info.plist` +
`project.pbxproj`, and `build.gradle` — they will drift the first time
one is bumped by hand. Pick one source (`app.json` + `expo prebuild`, or
a `make mobile-bump`) and write the rule into MOBILE.md. Separately: the
repo's CI gates run the Rust suite, but nothing runs `npm run typecheck`
and `npm test` for `mobile/` — both finish in seconds and would have
caught any of §3's regressions the day they were introduced. Add them as
a job (Node 22, `npm ci --prefix mobile`).

**Partially resolved 2026-08-06:** CI now uses Node 22 and runs lockfile-clean
installation, the strict typecheck, all Jest tests, and the Android
release-signing refusal check. ESLint/Prettier and a single authoritative
version-bump path remain open.

### 7.5 Doc drift: MOBILE.md contradicts the repo about native dirs

MOBILE.md says the first build "creates ignored `ios/` or `android/`
directories" and that `app.json` + TypeScript are "the committed source
of truth." But both native directories **are** committed, deliberately —
`mobile/.gitignore` explains they're versioned so the projects open
directly in Xcode/Android Studio. That leaves the real rule unstated:
when `app.json` and a committed native file disagree (they already
mildly do: `MARKETING_VERSION = 1.0` in `project.pbxproj` vs `1.0.0` in
`app.json` and `Info.plist`), which wins?
State it in MOBILE.md: "config plugins and `app.json` are authoritative;
after changing them run `npx expo prebuild --clean` and commit the
regenerated native dirs" — and fix the "ignored" sentence. A doc that
contradicts the tree it ships in costs trust beyond this one paragraph.

**Resolved 2026-08-06:** MOBILE.md now states that the native projects are
committed, defines `app.json` plus config plugins as the configuration source,
and requires a clean prebuild plus a committed native-project diff after
configuration changes.

---

## 8. The TV gap — what Google TV and Apple TV actually require

### 8.1 Where it stands

| Target | Buildable from this repo today? | Hard blockers |
|---|---|---|
| iPhone / iPad | ✅ yes | — |
| Android phone/tablet | ✅ yes | — |
| Apple TV (tvOS) | ❌ no | React Native core has no tvOS support; requires the `react-native-tvos` fork + tvOS target |
| Google TV / Android TV | ⚠️ sideload-only, degraded | No `LEANBACK_LAUNCHER` intent (invisible in the TV launcher), no `android:banner`, touchscreen not declared optional (Play won't offer it to TVs), no focus-visible styles, 7–9pt text at 3 m |

A sideloaded APK on Google TV will run, but it doesn't appear in the
launcher row without a third-party launcher, nothing on screen shows
where D-pad focus is (styles handle `pressed` only — `focused` appears
nowhere in the codebase), and the Add flow is dead because TV has no
usable document-picker surface.

### 8.2 The path (and it's shorter than it looks)

The ecosystem is aligned for exactly this stack: **`react-native-tvos`
`0.86.0-2` matches the app's React Native `0.86.2`**, and Expo supports TV
targets via `EXPO_TV=1` prebuild plus the `@react-native-tvos/config-tv`
plugin (v0.1.6) — see Expo's [Building for TV guide](https://docs.expo.dev/guides/building-for-tv/).
Concretely:

1. `"react-native": "npm:react-native-tvos@0.86.0-2"` in `package.json`,
   add `@react-native-tvos/config-tv`, prebuild with `EXPO_TV=1`. This
   yields the tvOS target and TV-flavored Android in the same codebase;
   phone builds are unaffected when the env var is off.
2. Android TV manifest (the config plugin does most of it):
   `LEANBACK_LAUNCHER` category, `android:banner` (320×180 asset — the
   noirr `n_` tile is a natural fit), `uses-feature
   android.hardware.touchscreen required=false` and `leanback
   required=false` so one APK serves both form factors.
3. Focus is the real work, and it's shared-screen work, not a fork:
   - every `Pressable` gains a visible `focused` style (the style
     callback already receives `pressed`; add `focused` the same way) —
     on TV this is *the* navigation affordance;
   - a 10-foot type scale: the §6.1 ramp × ~2 on TV
     (`Platform.isTV`), minimum ~24dp for body text, and 5% overscan
     margins on screen edges;
   - `hasTVPreferredFocus` on the queue's first card per screen so focus
     lands somewhere sensible;
   - kill the modals-from-bottom pattern on TV (sheet + Switch +
     TextInput is hostile to D-pad) in favor of full-screen focused
     panels.
4. The Add flow on TV becomes URL-based: no Files app, no share sheet —
   `POST /api/v1/jobs?url=` (already in the server) plus a paste/URL
   field, or simply *no add on TV* (see below).

### 8.3 Recommendation: ship the read-mostly TV mode first

The honest TV use case for a downloader is glanceability: the queue wall
— what's downloading, how fast, storage headroom, provider health — on
the room's biggest screen. Pause/resume and per-job pause cover 90% of
couch interventions. Recommend a first TV milestone that is exactly the
Dashboard queue tab + overview dock with focus styles and TV type,
explicitly excluding Add, History detail, and Logs. That milestone is
mostly §6.1 + §4.1 work you want anyway, plus the fork swap — the TV
investment then rides improvements the phone app needs regardless. The
§4.1 virtualization work is a prerequisite in practice: TV hardware GPUs
are weaker than flagship phones, and the 1 Hz full-tree re-render will
show there first.

---

## 9. Tests — covered vs. not

Current: 41 tests across 6 suites, all pure logic, all passing. The
choices are good — they pin wire shapes and the fragile parsing joints,
which is where regressions are silent:

| Covered | Suite |
|---|---|
| SSE chunk reassembly, CRLF splits, multiline data, keepalives | `sse-test` |
| URL normalization incl. IPv6 brackets, credential rejection | `format-test` |
| Discovery host selection (link-local rejection, IPv6, TXT) | `discovery-test` |
| Queue sectioning, status-enum shapes, storage math | `queue-sections`, `format` |
| Priority bands, theme resolution | `priority`, `theme` |

Not covered — in order of risk:

1. **The `useNzbd` state machine** (reconnect, backoff reset, stale-poll
   trigger, `reset`/`lagged` → refresh, disposal). This is the most
   intricate logic in the app and has zero tests. It's testable without a
   device: jest fake timers + a scripted `openEventStream` mock feeding
   the real `SseParser`. Five scenarios would pin the whole lifecycle.
2. **History pagination merge** (§3.2) — a two-page fixture with an
   insertion between fetches.
3. **Auth header building** — token precedence over basic, and the §3.1
   UTF-8 case.
4. **`formatDuration` boundaries** (§3.6).

Also absent: any linting. The Rust side runs `cargo fmt` in a pre-commit
hook; `mobile/` has no eslint/prettier at all. `eslint-config-expo` +
prettier, wired into the same CI job as §7.4, matches the discipline the
rest of the repo already has.

---

## 10. What this review did not do

No on-device profiling (the §4 findings are static analysis of render
paths plus the verified server tick rate — profile before optimizing
beyond the structural fixes); no Xcode signing/provisioning audit; no
store-listing asset review; no review of the Rust server beyond the API
contract surface the app touches; no dependency audit beyond noting the
lockfile pins and the single third-party native module
(`@inthepocket/react-native-service-discovery` — worth a periodic glance,
since discovery is the one capability with no first-party fallback).

---

## 11. Priority punch list

| P | Item | Status | Effort | Section |
|---|---|---|---|---|
| P0 | Real release keystore (blocks any distribution) | Complete 2026-08-06 | S | §7.1 |
| P0 | UTF-8 basic-auth encoding | Complete 2026-08-06 | S | §3.1 |
| P1 | Virtualize lists + memoized rows | Open | M | §4.1 |
| P1 | `.nzb` open-in / share-sheet intake (+ `url=` add) | Open | M | §5.1 |
| P1 | 401 → stop retrying, offer Edit connection | Open | S | §5.2 |
| P1 | History pagination dedupe | Open | S | §3.2 |
| P1 | Mobile typecheck+jest in CI; eslint | Partial: CI complete; lint open | S | §7.4, §9 |
| P2 | Logs via SSE `log` events (or `after` cursor) | Open | S–M | §4.2 |
| P2 | Type ramp ≥10pt + Dynamic Type policy | Open | M | §6.1 |
| P2 | ENOSPC / health-abort / speed-limit in guard banner | Open | S | §5.3 |
| P2 | Refresh-vs-tick guard; scoped busy state | Open | S | §3.3, §4.3 |
| P2 | `https` port default fix; upload timeout | Open | S | §3.4, §3.5 |
| P2 | MOBILE.md native-dirs correction + prebuild rule | Complete 2026-08-06 | S | §7.5 |
| P3 | noirr token adoption (shared with web UI) | Open | M–L | §6.2 |
| P3 | TV: tvos fork + config-tv + read-mostly TV mode | Open | L | §8 |
| P3 | Requeue/restore from mobile history | Open | S | §5.4 |

S ≈ under an hour · M ≈ an afternoon · L ≈ multi-day. P0 before anyone
else installs a build; P1 before calling it 1.0; P2 opportunistic; P3
are direction decisions.
