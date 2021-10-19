package main

import (
	"fmt"
	"net"
	"os"

	service "dapr-external-state/service"

	statev1pb "github.com/dapr/components-contrib/state/proto/v1"
	"github.com/dapr/components-contrib/state/redis"

	"github.com/dapr/kit/logger"
	"google.golang.org/grpc"
)

/*
 This is an experiment to create an external state store for Dapr.
 ---------
 This code was hacked together in a couple of hours and should only be played with.
*/
const (
	EXT_SS_PORT_ENV_VAR_NAME = "EXT_SS_PORT"
	DEFAULT_EXT_SS_PORT      = "9191"
)

func main() {
	port := os.Getenv(EXT_SS_PORT_ENV_VAR_NAME)
	if port == "" {
		port = DEFAULT_EXT_SS_PORT
	}
	fmt.Printf("External state store listening on: %s\n", port)

	// We inject a redis state store here but in theory this could by any of the existing state stores.
	stateStore := redis.NewRedisStateStore(logger.NewLogger("redis"))
	storeService := service.NewStoreService(stateStore)

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", port))
	if err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	}

	// TODO:
	// * Add security
	// * Add tracing
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	statev1pb.RegisterStoreServer(grpcServer, storeService)
	err = grpcServer.Serve(lis)
	if err != nil {
		panic(err)
	}
}
