package web

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestServerRejectsManagementRoutesByDefault(t *testing.T) {
	server := New(nil, nil)

	for _, method := range []string{http.MethodGet, http.MethodPost} {
		req := httptest.NewRequest(method, "/api/stats", nil)
		res := httptest.NewRecorder()
		server.ServeHTTP(res, req)

		if res.Code != http.StatusUnauthorized {
			t.Fatalf("%s /api/stats status = %d, want %d", method, res.Code, http.StatusUnauthorized)
		}
	}
}

func TestServerUsesExplicitAuthMiddleware(t *testing.T) {
	server := New(nil, nil)
	server.UseAuth(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer test" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	})

	res := httptest.NewRecorder()
	server.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/", nil))
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated request status = %d, want %d", res.Code, http.StatusUnauthorized)
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer test")
	res = httptest.NewRecorder()
	server.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("authenticated request status = %d, want %d", res.Code, http.StatusOK)
	}
}

func TestCSRFMiddlewareProtectsStateChangingMethods(t *testing.T) {
	server := New(nil, nil)
	server.UseAuth(func(next http.Handler) http.Handler { return next })
	server.UseCSRF()

	get := httptest.NewRequest(http.MethodGet, "/", nil)
	getRes := httptest.NewRecorder()
	server.ServeHTTP(getRes, get)
	if getRes.Code != http.StatusOK {
		t.Fatalf("dashboard status = %d, want %d", getRes.Code, http.StatusOK)
	}
	cookie := getRes.Result().Cookies()[0]
	if cookie.Name != csrfCookieName || cookie.Value == "" {
		t.Fatalf("missing csrf cookie: %#v", cookie)
	}

	post := httptest.NewRequest(http.MethodPost, "/not-a-management-route", strings.NewReader("{}"))
	post.AddCookie(cookie)
	postRes := httptest.NewRecorder()
	server.ServeHTTP(postRes, post)
	if postRes.Code != http.StatusForbidden {
		t.Fatalf("csrf-less POST status = %d, want %d", postRes.Code, http.StatusForbidden)
	}

	post.Header.Set("X-CSRF-Token", cookie.Value)
	postRes = httptest.NewRecorder()
	server.ServeHTTP(postRes, post)
	if postRes.Code == http.StatusForbidden {
		t.Fatal("valid csrf token was rejected")
	}
}
