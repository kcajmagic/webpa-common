package xresolver

import (
	"context"
	"errors"
	"github.com/go-kit/kit/log"
	"github.com/xmidt-org/webpa-common/logging"
	"net"
	"strconv"
	"sync"
)

// Note to self: Dial is not being set for net.Resolver because that is the Dial to the DNS server.

var DefaultDialer = net.Dialer{}

type resolver struct {
	resolvers map[Lookup]bool
	lock      sync.RWMutex
	dialer    net.Dialer
	logger    log.Logger
}

func NewResolver(dialer net.Dialer, logger log.Logger, lookups ...Lookup) Resolver {
	if logger == nil {
		logger = logging.DefaultLogger()
	}

	r := &resolver{
		resolvers: make(map[Lookup]bool),
		dialer:    dialer,
		logger:    logger,
	}

	for _, lookup := range lookups {
		r.Add(lookup)
	}
	return r
}

func (resolve *resolver) Add(r Lookup) error {
	resolve.lock.RLock()
	found := resolve.resolvers[r]
	resolve.lock.RUnlock()
	if found {
		return errors.New("resolver already exist")
	}

	resolve.lock.Lock()
	resolve.resolvers[r] = true
	resolve.lock.Unlock()
	return nil
}

func (resolve *resolver) Remove(r Lookup) error {
	resolve.lock.RLock()
	found := resolve.resolvers[r]
	resolve.lock.RUnlock()
	if !found {
		return errors.New("resolver does not exist")
	}

	resolve.lock.Lock()
	delete(resolve.resolvers, r)
	resolve.lock.Unlock()
	return nil
}

func (resolve *resolver) getRoutes(ctx context.Context, host string) []Route {
	routes := make([]Route, 0)
	for r := range resolve.resolvers {
		tempRoutes, err := r.LookupRoutes(ctx, host)
		logging.Debug(resolve.logger).Log(logging.MessageKey(), "completed Lookup", "temp route", tempRoutes, "err", err)
		if err == nil {
			routes = append(routes, tempRoutes...)
		}
	}

	return routes
}

func (resolve *resolver) DialContext(ctx context.Context, network, addr string) (con net.Conn, err error) {
	logging.Debug(resolve.logger).Log(logging.MessageKey(), "Dialing", "network", network, "addr", addr)
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ip := net.ParseIP(host)
	if ip != nil {
		return resolve.dialer.Dial(network, net.JoinHostPort(ip.String(), port))
	}

	// get records using custom resolvers
	routes := resolve.getRoutes(ctx, host)

	// generate Conn or err from records
	con, err = resolve.createConnection(routes, network, port)
	if err == nil {
		return
	} else {
		logging.Error(resolve.logger).Log(logging.MessageKey(), "createConnection: failed to create connection", logging.ErrorKey(), err)
	}

	logging.Info(resolve.logger).Log(logging.MessageKey(), "using default address")
	// if no connection, create using the default dialer
	return resolve.dialer.DialContext(ctx, network, addr)
}

func (resolve *resolver) createConnection(routes []Route, network, port string) (con net.Conn, err error) {
	logging.Debug(resolve.logger).Log(logging.MessageKey(), "resolver create connection", "routes", routes, "network", network, "port", port)
	for _, route := range routes {
		portUsed := port
		if route.Port != 0 {
			portUsed = strconv.Itoa(route.Port)
		}
		con, err = resolve.dialer.Dial(network, net.JoinHostPort(route.Host, portUsed))
		if err == nil {
			return
		} else {
			logging.Error(resolve.logger).Log("failed to dial connection", logging.ErrorKey(), err)
		}
	}
	return nil, errors.New("failed to create connection from routes")
}
