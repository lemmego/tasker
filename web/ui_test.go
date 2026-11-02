package web

import (
	"bufio"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lemmego/tasker"
)

// openServer returns a dashboard that lets everything through, over a
// manager whose stats calls answer.
//
// The driver is not optional here: a Manager built without one panics on the
// first stats read, which is how the first version of the stream test failed
// — the handler was correct and the fixture was nil.
func openServer(t *testing.T, driver ...*stubDriver) *Server {
	t.Helper()
	d := &stubDriver{}
	if len(driver) > 0 {
		d = driver[0]
	}
	s := New(tasker.NewManager(tasker.WithDriver(d)), nil)
	s.UseAuth(func(next http.Handler) http.Handler { return next })
	return s
}

// TestTheDashboardCarriesItsOwnThemeKey.
//
// observe's dashboard remembers a theme under observe.theme, and the two are
// routinely mounted on one origin — productshore serves /observe and /tasker
// from the same host. localStorage is per-origin, not per-path, so an
// unnamespaced key would make choosing dark here silently change observe and
// the other way round.
func TestTheDashboardCarriesItsOwnThemeKey(t *testing.T) {
	if !strings.Contains(dashboardThemeJS, "tasker.theme") {
		t.Error("the theme is not remembered under a tasker-namespaced key")
	}
	if strings.Contains(dashboardThemeJS, "observe.theme") ||
		strings.Contains(dashboardJS, "observe.theme") {
		t.Error("the dashboard reads or writes observe's theme key, so the two " +
			"dashboards on one origin would fight over it")
	}
	if !strings.Contains(dashboardJS, "tasker.theme") {
		t.Error("the theme toggle does not persist under the namespaced key")
	}
}

// TestTheThemeIsAppliedBeforeTheBody.
//
// A theme applied after the body has rendered is a white flash on every load
// for somebody who chose dark, and that flash is the thing people notice
// about a theme switcher. The only way to avoid it is for the script to run
// in the head, so that is what is asserted — the position, not the content.
func TestTheThemeIsAppliedBeforeTheBody(t *testing.T) {
	head := strings.Index(dashboardHTML, "</head>")
	theme := strings.Index(dashboardHTML, "tasker.theme")
	body := strings.Index(dashboardHTML, "<body>")
	if theme < 0 || head < 0 || body < 0 {
		t.Fatal("the document is not shaped as expected")
	}
	if theme > head {
		t.Error("the theme script runs after </head>, so a dark-mode load flashes white")
	}
	if theme > body {
		t.Error("the theme script runs inside the body")
	}
}

// TestTheDashboardRendersItsFrame, so a refactor that drops an element the
// script addresses by id is caught here rather than as a blank page.
func TestTheDashboardRendersItsFrame(t *testing.T) {
	res := httptest.NewRecorder()
	openServer(t).ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/", nil))
	if res.Code != http.StatusOK {
		t.Fatalf("dashboard status = %d, want 200", res.Code)
	}
	body := res.Body.String()

	// Every id the script calls el() on. A missing one is a TypeError on the
	// first line that touches it, which takes the whole dashboard with it.
	for _, id := range []string{
		"nav", "title", "subtitle", "bar", "list", "detail", "notices",
		"q", "queue", "kind", "reload", "counts", "live", "prune", "theme",
		"toast", "status",
	} {
		if !strings.Contains(body, `id="`+id+`"`) {
			t.Errorf("the document has no element with id %q, which the script addresses", id)
		}
	}
	// And the light palette, since a dashboard that only has a dark theme
	// with a switcher that appears to work is worse than no switcher.
	if !strings.Contains(body, `:root[data-theme="light"]`) {
		t.Error("there is no explicit light palette")
	}
	if !strings.Contains(body, "prefers-color-scheme: light") {
		t.Error("the stylesheet does not follow the operating system")
	}
}

// TestTheMainPaneScrollsRatherThanTheDocument.
//
// This is the macOS rubber-band fix and it is two rules, not one: the
// document must not scroll, and the inner pane must not chain its overscroll
// back to the document. With only the first, a scroll that reaches the end of
// the list bounces the document instead — which looks identical to the
// problem it was meant to fix.
func TestTheMainPaneScrollsRatherThanTheDocument(t *testing.T) {
	if !strings.Contains(dashboardCSS, "overscroll-behavior:contain") {
		t.Error("the scrolling pane does not contain its overscroll, so a bounce " +
			"chains up to the document and drags the sticky header with it")
	}
	if !strings.Contains(dashboardCSS, "html,body{height:100%;overflow:hidden") {
		t.Error("the document itself still scrolls, which is what rubber-bands")
	}
	if !strings.Contains(dashboardCSS, "html{background:var(--bg)}") {
		t.Error("html has no background, so any bounce that does happen shows white")
	}
}

// TestAJobListingIsBounded.
//
// The limit arrives from a query string and was passed through untouched, so
// limit=1000000 was a request for a million rows the database would honour.
func TestAJobListingIsBounded(t *testing.T) {
	for _, c := range []struct {
		asked, want int
	}{
		{1000000, MaxPageSize},
		{MaxPageSize + 1, MaxPageSize},
		{MaxPageSize, MaxPageSize},
		{100, 100},
		{1, 1},
		// Zero and negative mean "unspecified" rather than "none": a caller
		// asking for no rows built the URL wrong, and an empty list reads as
		// an empty queue.
		{0, DefaultPageSize},
		{-1, DefaultPageSize},
		{-1000000, DefaultPageSize},
	} {
		if got := clampLimit(c.asked); got != c.want {
			t.Errorf("clampLimit(%d) = %d, want %d", c.asked, got, c.want)
		}
	}
}

// writerWithoutFlush wraps a ResponseWriter and implements neither Flusher
// nor Hijacker, declaring only Unwrap.
//
// This is what middleware in this framework actually looks like, and it is
// the whole reason the stream was broken: embedding promotes exactly three
// methods, so w.(http.Flusher) was false for every request behind
// lemmego's RequestLogger. http.ResponseController follows Unwrap; a type
// assertion traverses nothing.
type writerWithoutFlush struct{ inner http.ResponseWriter }

func (w writerWithoutFlush) Header() http.Header         { return w.inner.Header() }
func (w writerWithoutFlush) Write(b []byte) (int, error) { return w.inner.Write(b) }
func (w writerWithoutFlush) WriteHeader(code int)        { w.inner.WriteHeader(code) }
func (w writerWithoutFlush) Unwrap() http.ResponseWriter { return w.inner }

// TestTheStreamWorksBehindAWrappingWriter.
//
// Verified to fail against the committed version, which answered
// 500 {"error":"streaming not supported"} — the exact response productshore
// got from this endpoint for as long as it existed.
func TestTheStreamWorksBehindAWrappingWriter(t *testing.T) {
	dash := openServer(t)
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dash.ServeHTTP(writerWithoutFlush{inner: w}, r)
	})
	srv := httptest.NewServer(wrapped)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/api/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("stream behind a wrapping writer = %d, want 200", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}
	// nginx buffers text/event-stream by default, so without this the stream
	// looks dead through a proxy and nowhere else.
	if res.Header.Get("X-Accel-Buffering") != "no" {
		t.Error("X-Accel-Buffering is not disabled, so a proxy will buffer the stream")
	}
}

// TestTheStreamSaysSomethingImmediately.
//
// The timing is the assertion, and it has to be: the heartbeat and the tick
// both eventually write, so a test that merely waits for bytes would pass
// against a handler that flushes nothing — which is the version that made a
// working stream indistinguishable from a hung one for five seconds.
//
// Asserting under two seconds against a five-second tick and a twenty-second
// heartbeat leaves no reading where a late write could satisfy it.
func TestTheStreamSaysSomethingImmediately(t *testing.T) {
	srv := httptest.NewServer(openServer(t))
	defer srv.Close()

	started := time.Now()
	res, err := http.Get(srv.URL + "/api/events")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	type read struct {
		line string
		err  error
	}
	got := make(chan read, 1)
	go func() {
		line, err := bufio.NewReader(res.Body).ReadString('\n')
		got <- read{line: line, err: err}
	}()

	select {
	case r := <-got:
		if r.err != nil {
			t.Fatalf("reading the first frame: %v", r.err)
		}
		if elapsed := time.Since(started); elapsed > 2*time.Second {
			t.Errorf("the first frame took %v; it is being written by the 5s tick "+
				"rather than sent on connect", elapsed)
		}
		// A named event, so a client can listen for this one specifically
		// rather than for whatever arrives.
		if !strings.HasPrefix(r.line, "event: stats") {
			t.Errorf("first frame = %q, want an \"event: stats\" line", r.line)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("nothing arrived within 3s, so the stream never flushed on connect")
	}
}

// TestAFailedStatsReadDoesNotDropTheStream.
//
// A reconnect loop is worse than a gap: the browser retries, the handler
// fails again, and the database gets a stats query per retry for as long as
// whatever broke stays broken. A comment frame keeps the socket warm until
// the next tick, which may well succeed.
func TestAFailedStatsReadDoesNotDropTheStream(t *testing.T) {
	broken := &stubDriver{stats: func(context.Context) (*tasker.GlobalStats, error) {
		return nil, errors.New("database is down")
	}}
	srv := httptest.NewServer(openServer(t, broken))
	defer srv.Close()

	res, err := http.Get(srv.URL + "/api/events")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 — a failing stats read must not refuse "+
			"the connection", res.StatusCode)
	}

	type read struct {
		line string
		err  error
	}
	got := make(chan read, 1)
	go func() {
		line, err := bufio.NewReader(res.Body).ReadString('\n')
		got <- read{line: line, err: err}
	}()

	select {
	case r := <-got:
		if r.err != nil {
			t.Fatalf("the stream was dropped rather than held open: %v", r.err)
		}
		// A comment, which is a valid SSE frame carrying no event.
		if !strings.HasPrefix(r.line, ":") {
			t.Errorf("frame = %q, want a comment frame", r.line)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("nothing arrived, so a failing stats read produces silence")
	}
}

// TestAProcessWithNoSupervisorStillServesTheDashboard.
//
// New(manager, nil) is a public construction and this package's own security
// tests use it. Every supervisor-backed endpoint used to be a nil pointer
// dereference — which the api recoverer turns into a 500 on the one endpoint
// the overview cannot render without, so the whole first screen read
// "Failed to fetch". Found by screenshotting the dashboard rather than by
// any test, which is the argument for having looked at it.
func TestAProcessWithNoSupervisorStillServesTheDashboard(t *testing.T) {
	s := New(tasker.NewManager(tasker.WithDriver(&stubDriver{})), nil)
	s.UseAuth(func(next http.Handler) http.Handler { return next })

	// Reads answer emptily rather than failing: "this process runs no
	// workers" is not an error.
	for _, path := range []string{"/api/queues", "/api/metrics/queues"} {
		res := httptest.NewRecorder()
		s.ServeHTTP(res, httptest.NewRequest(http.MethodGet, path, nil))
		if res.Code != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", path, res.Code)
		}
		if body := strings.TrimSpace(res.Body.String()); body != "[]" {
			t.Errorf("GET %s body = %q, want an empty array", path, body)
		}
	}

	// Writes refuse, and say why. A 500 would read as a broken dashboard;
	// this is a dashboard that cannot do a thing and names it.
	for _, path := range []string{"/api/queues/default/pause", "/api/queues/default/resume"} {
		res := httptest.NewRecorder()
		s.ServeHTTP(res, httptest.NewRequest(http.MethodPost, path, nil))
		if res.Code != http.StatusConflict {
			t.Errorf("POST %s = %d, want 409", path, res.Code)
		}
		if !strings.Contains(res.Body.String(), "runs no workers") {
			t.Errorf("POST %s body = %q, want it to say why", path, res.Body.String())
		}
	}
}

// TestAZeroTimestampIsNotRenderedAsAncient.
//
// Go marshals an unset time.Time as 0001-01-01T00:00:00Z, which Date.parse
// handles perfectly well — so the job detail showed "SCHEDULED: 739921d ago"
// for a job that was never scheduled. Found by screenshotting the page, not
// by any assertion, which is the argument for having looked at it.
//
// This pins the guard rather than the behaviour: the logic is JavaScript in
// a string, and a Go test cannot execute it without putting node in this
// module's toolchain. Deleting the guard fails this; breaking it subtly
// would not, and that is stated rather than implied.
func TestAZeroTimestampIsNotRenderedAsAncient(t *testing.T) {
	if !strings.Contains(dashboardJS, "if (then < 0) return '';") {
		t.Error("ago() no longer floors at the epoch, so an unset timestamp " +
			"renders as a relative time two thousand years in the past")
	}
	// And the fact that would display it is dropped when empty, so an absent
	// value is an absent row rather than a label over a blank.
	if !strings.Contains(dashboardJS, "if (v === '' || v === null || v === undefined) return '';") {
		t.Error("fact() renders empty values, so a job with no node or batch " +
			"shows labelled blanks")
	}
}

// TestTheFirstPaintDoesNotFlashEveryRow.
//
// Every row is unseen on the first render, so without this the whole table
// lights up at once — which says "all of this is new" about a list that is
// mostly hours old, and is the opposite of what the highlight is for.
func TestTheFirstPaintDoesNotFlashEveryRow(t *testing.T) {
	if !strings.Contains(dashboardJS, "state.painted && !state.seen[j.id]") {
		t.Error("the fresh-row highlight is not gated on a prior paint")
	}
	if !strings.Contains(dashboardJS, "state.painted = false;") {
		t.Error("switching panels does not reset the paint flag, so the next " +
			"panel's first render flashes everything")
	}
}
