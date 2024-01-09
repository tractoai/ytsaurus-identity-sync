package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

const defaultHTTPProxyPort = 10110

type YtsaurusLocal struct {
	containerRequest testcontainers.ContainerRequest
	container        testcontainers.Container

	httpProxyPort int
}

func NewYtsaurusLocal() *YtsaurusLocal {
	return NewYtsaurusLocalCustom(
		defaultHTTPProxyPort,
		BuildCustomContainerRequest(defaultHTTPProxyPort),
	)
}

func NewYtsaurusLocalCustom(proxyPort int, request testcontainers.ContainerRequest) *YtsaurusLocal {
	return &YtsaurusLocal{
		httpProxyPort:    proxyPort,
		containerRequest: request,
	}
}

func BuildCustomContainerRequest(proxyPort int) testcontainers.ContainerRequest {
	return testcontainers.ContainerRequest{
		Image: "ytsaurus/local:stable",
		ExposedPorts: []string{
			strconv.Itoa(proxyPort) + ":80/tcp", // http
		},
		WaitingFor: wait.ForLog("Local YT started").WithStartupTimeout(3 * time.Minute),
		Cmd: []string{
			"--fqdn",
			"localhost",
			"--proxy-config",
			fmt.Sprintf("{address_resolver={enable_ipv4=%%true;enable_ipv6=%%false;};coordinator={public_fqdn=\"localhost:%d\"}}", proxyPort),
			"--enable-debug-logging",
		},
	}
}

func (y *YtsaurusLocal) Start() error {
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: y.containerRequest,
		Started:          true,
	})
	if err != nil {
		return err
	}
	y.container = container
	return nil
}

func (y *YtsaurusLocal) Stop() error {
	ctx := context.Background()
	if y.container == nil {
		return errors.New("container not started")
	}
	err := y.container.Terminate(ctx)
	if err != nil {
		return err
	}
	y.container = nil
	return nil
}

func (y *YtsaurusLocal) GetProxy() string {
	return "localhost:" + strconv.Itoa(y.httpProxyPort)
}

func (y *YtsaurusLocal) GetToken() string {
	return "password"
}

func (y *YtsaurusLocal) GetClient() (yt.Client, error) {
	return ythttp.NewClient(&yt.Config{
		Proxy: y.GetProxy(),
		Credentials: &yt.TokenCredentials{
			Token: "password",
		},
	})
}
