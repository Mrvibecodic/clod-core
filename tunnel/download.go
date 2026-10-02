package tunnel

import (
	"sort"

	C "github.com/metacubex/mihomo/constant"
)

// DownloadPingURL is the probe address for the plain probe after an empty
// download check through p: that of p's provider, else of the first group
// that has p among its own proxies, else the default one.
func DownloadPingURL(p C.Proxy) string {
	configMux.RLock()
	all := providers
	configMux.RUnlock()

	if pd, ok := all[p.ProxyInfo().ProviderName]; ok && pd.HealthCheckURL() != "" {
		return pd.HealthCheckURL()
	}
	names := make([]string, 0, len(all))
	for name := range all {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		pd := all[name]
		if pd.HealthCheckURL() == "" {
			continue
		}
		for _, member := range pd.Proxies() {
			if member == p {
				return pd.HealthCheckURL()
			}
		}
	}
	return C.DefaultTestURL
}
