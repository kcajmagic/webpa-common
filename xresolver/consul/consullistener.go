package consul

import (
	"context"
	"errors"
	"github.com/go-kit/kit/log"
	"github.com/xmidt-org/webpa-common/logging"
	"github.com/xmidt-org/webpa-common/service/monitor"
	"github.com/xmidt-org/webpa-common/xresolver"
	"regexp"
	"net/url"
)

var find = regexp.MustCompile("(.*)" + regexp.QuoteMeta("[") + "(.*)" + regexp.QuoteMeta("]") + regexp.QuoteMeta("{") + "(.*)" + regexp.QuoteMeta("}"))

type Options struct {
	// Watch is what to url to match with the consul service
	// exp. { "beta.google.com" : "caduceus" }
	Watch map[string]string `json:"watch"`

	Logger log.Logger `json:"-"`
}

type ConsulWatcher struct {
	logger log.Logger

	config Options

	watch     map[string]string
	balancers map[string]*xresolver.RoundRobin
}

func NewConsulWatcher(o Options) *ConsulWatcher {
	if o.Logger == nil {
		o.Logger = logging.DefaultLogger()
	}

	watcher := &ConsulWatcher{
		logger: log.WithPrefix(o.Logger, "component", "consulwatcher"),

		config: o,

		balancers: make(map[string]*xresolver.RoundRobin),
		watch:     make(map[string]string),
	}

	if o.Watch != nil {
		for url, service := range o.Watch {

			watcher.WatchService(url, service)
		}
	}

	return watcher
}

func (watcher *ConsulWatcher) MonitorEvent(e monitor.Event) {
	logging.Debug(watcher.logger).Log(logging.MessageKey(), "received update route event", "event", e)

	// update balancers
	str := find.FindStringSubmatch(e.Key)
	if len(str) < 3 {
		return
	}

	service := str[1]
	if rr, found := watcher.balancers[service]; found {
		routes := make([]xresolver.Route, len(e.Instances))
		for index, instance := range e.Instances {
			// find records
			route, err := xresolver.CreateRoute(instance)
			if err != nil {
				logging.Error(watcher.logger).Log(logging.MessageKey(), "failed to create route", logging.MessageKey(), err, "instance", instance)
				continue
			}
			routes[index] = route
		}
		rr.Update(routes)
		logging.Info(watcher.logger).Log(logging.MessageKey(), "updating routes", "service", service, "new-routes", routes)
	}
}

func (watcher *ConsulWatcher) WatchService(watchURL string, service string) {
	url, err := url.Parse(watchURL)
	if err != nil{
		logging.Error(watcher.logger).Log("Failed to parse url", "url", watchURL)
		return
	}

	logging.Debug(watcher.logger).Log(logging.MessageKey(), "Watch Service", "url", watchURL, "service", service, "host", url.Host)
	if _, found := watcher.watch[url.Host]; !found {
		watcher.watch[url.Host] = service
		if _, found := watcher.balancers[service]; !found {
			logging.Debug(watcher.logger).Log(logging.MessageKey(), "Creating round robin balancer", "url", url, "service", service)
			watcher.balancers[service] = xresolver.NewRoundRobinBalancer()
		}
	}
}

func (watcher *ConsulWatcher) LookupRoutes(ctx context.Context, host string) ([]xresolver.Route, error) {
	if _, found := watcher.config.Watch[host]; !found {
		logging.Error(watcher.logger).Log(logging.MessageKey(), "LookupRoutes: host not found in config", "host", host)
		return []xresolver.Route{}, errors.New(host + " is not part of the consul listener")
	}
	records, err := watcher.balancers[watcher.config.Watch[host]].Get()
	logging.Debug(watcher.logger).Log(logging.MessageKey(), "looking up routes", "routes", records, logging.ErrorKey(), err)
	return records, err
}
