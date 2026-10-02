package route

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outboundgroup"
	"github.com/metacubex/mihomo/adapter/provider"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/profile/cachefile"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel"

	"github.com/metacubex/chi"
	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

var (
	SwitchProxiesCallback func(sGroup string, sProxy string)
)

func proxyRouter() http.Handler {
	r := chi.NewRouter()
	r.Get("/", getProxies)

	r.Route("/{name}", func(r chi.Router) {
		r.Use(parseProxyName, findProxyByName)
		r.Get("/", getProxy)
		r.Get("/delay", getProxyDelay)
		r.Get("/download", getProxyDownload)
		r.Put("/", updateProxy)
		r.Delete("/", unfixedProxy)
	})
	return r
}

func parseProxyName(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := getEscapeParam(r, "name")
		ctx := context.WithValue(r.Context(), CtxKeyProxyName, name)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func findProxyByName(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := r.Context().Value(CtxKeyProxyName).(string)
		proxies := tunnel.Proxies()
		proxy, exist := proxies[name]
		if !exist {
			render.Status(r, http.StatusNotFound)
			render.JSON(w, r, ErrNotFound)
			return
		}

		ctx := context.WithValue(r.Context(), CtxKeyProxy, proxy)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func getProxies(w http.ResponseWriter, r *http.Request) {
	proxies := tunnel.Proxies()
	render.JSON(w, r, render.M{
		"proxies": proxies,
	})
}

func getProxy(w http.ResponseWriter, r *http.Request) {
	proxy := r.Context().Value(CtxKeyProxy).(C.Proxy)
	render.JSON(w, r, proxy)
}

func updateProxy(w http.ResponseWriter, r *http.Request) {
	req := struct {
		Name string `json:"name"`
	}{}
	if err := render.DecodeJSON(r.Body, &req); err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, ErrBadRequest)
		return
	}

	proxy := r.Context().Value(CtxKeyProxy).(C.Proxy)
	selector, ok := proxy.Adapter().(outboundgroup.SelectAble)
	if !ok {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("Must be a Selector"))
		return
	}

	if err := selector.Set(req.Name); err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError(fmt.Sprintf("Selector update error: %s", err.Error())))
		return
	}

	cachefile.Cache().SetSelected(proxy.Name(), req.Name)
	if SwitchProxiesCallback != nil {
		// refresh tray menu
		go SwitchProxiesCallback(proxy.Name(), req.Name)
	}
	render.NoContent(w, r)
}

func getProxyDelay(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	url := query.Get("url")
	timeout, err := strconv.ParseInt(query.Get("timeout"), 10, 16)
	if err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, ErrBadRequest)
		return
	}

	expectedStatus, err := utils.NewUnsignedRanges[uint16](query.Get("expected"))
	if err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, ErrBadRequest)
		return
	}

	proxy := r.Context().Value(CtxKeyProxy).(C.Proxy)

	// The probe of the scheduled check: a stalled probe gets a second one and
	// a failure is confirmed before the node is marked dead. It may wait up to
	// ProbeReserve for its turn to the host, then take the hedge delay and the
	// timeout; the client waits for the timeout and five seconds.
	attempt := time.Millisecond * time.Duration(timeout)
	ctx, cancel := context.WithTimeout(context.Background(), attempt+provider.ProbeHedgeDelay+C.ProbeReserve)
	defer cancel()

	delay, err := provider.ProbeNode(ctx, proxy, url, expectedStatus, attempt)
	if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, provider.ErrProbeDiscarded) {
		render.Status(r, http.StatusGatewayTimeout)
		render.JSON(w, r, ErrRequestTimeout)
		return
	}

	if err != nil || delay == 0 {
		render.Status(r, http.StatusServiceUnavailable)
		if err != nil && delay != 0 {
			render.JSON(w, r, err)
		} else {
			render.JSON(w, r, newError("An error occurred in the delay test"))
		}
		return
	}

	render.JSON(w, r, render.M{
		"delay": delay,
	})
}

// downloadChecker is a node parsed from a config or a provider: groups and
// the built-in proxies have no fingerprint.
type downloadChecker interface {
	Fingerprint() string
	DownloadCheck(ctx context.Context, url string, size int64, timeout, stall time.Duration, pingURL string) adapter.DownloadResult
}

// downloadMaxSize keeps a check from turning into a download.
const downloadMaxSize = 1 << 20

// getProxyDownload tells whether traffic passes the node: it downloads size
// bytes of url through it and answers ok, frozen (cut after the first
// kilobytes), dead (nothing passes) or unknown.
func getProxyDownload(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	target := query.Get("url")
	size, sizeErr := strconv.ParseInt(query.Get("size"), 10, 64)
	timeout, timeoutErr := strconv.ParseInt(query.Get("timeout"), 10, 64)
	stall, stallErr := strconv.ParseInt(query.Get("stall"), 10, 64)
	parsed, urlErr := url.Parse(target)
	if sizeErr != nil || timeoutErr != nil || stallErr != nil || urlErr != nil ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" ||
		size <= 0 || size > downloadMaxSize || timeout <= 0 || timeout > 60000 || stall <= 0 || stall > timeout {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, ErrBadRequest)
		return
	}

	proxy := r.Context().Value(CtxKeyProxy).(C.Proxy)
	node, ok := proxy.(downloadChecker)
	if !ok || node.Fingerprint() == "" {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("Must be a node"))
		return
	}

	// The check may wait for its turn to the host, then download, then
	// probe; a client that gives up stops it.
	attempt := time.Millisecond * time.Duration(timeout)
	ctx, cancel := context.WithTimeout(r.Context(), C.ProbeReserve+attempt+adapter.DownloadPingTimeout+C.ProbeReserve)
	defer cancel()

	result := node.DownloadCheck(ctx, target, size, attempt, time.Millisecond*time.Duration(stall), tunnel.DownloadPingURL(proxy))
	render.JSON(w, r, result)
}

func unfixedProxy(w http.ResponseWriter, r *http.Request) {
	proxy := r.Context().Value(CtxKeyProxy).(C.Proxy)
	if selectAble, ok := proxy.Adapter().(outboundgroup.SelectAble); ok && proxy.Type() != C.Selector {
		selectAble.ForceSet("")
		cachefile.Cache().SetSelected(proxy.Name(), "")
		render.NoContent(w, r)
		return
	}
	render.Status(r, http.StatusBadRequest)
	render.JSON(w, r, ErrBadRequest)
}
