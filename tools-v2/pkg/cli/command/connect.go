package basecmd

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"math"
	"sync"
	"time"
)

type ConnectionPool struct {
	connections map[string]*grpc.ClientConn
	mux         sync.RWMutex
}

func NewConnectionPool() *ConnectionPool {
	return &ConnectionPool{
		connections: make(map[string]*grpc.ClientConn),
	}
}

func (c *ConnectionPool) GetConnection(address string, timeout time.Duration) (*grpc.ClientConn, error) {
	c.mux.RLock()
	conn, ok := c.connections[address]
	c.mux.RUnlock()
	if ok {
		return conn, nil
	}
	log.Printf("%s: start to dial", address)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithInitialConnWindowSize(math.MaxInt32),
		grpc.WithInitialWindowSize(math.MaxInt32))
	if err != nil {
		log.Printf("%s: fail to dial", address)
		return nil, err
	}
	c.mux.Lock()
	c.connections[address] = conn
	c.mux.Unlock()

	return conn, nil
}

func (c *ConnectionPool) Release(address string) {
	c.mux.Lock()
	defer c.mux.Unlock()
	delete(c.connections, address)
}
