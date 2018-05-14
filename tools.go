// Copyright 2018 Andy Lo-A-Foe. All rights reserved.
// Use of this source code is governed by Apache-style
// license that can be found in the LICENSE file.
package rabbitmq

import (
	"errors"

	cfenv "github.com/cloudfoundry-community/go-cfenv"
)

func uriFromService(service *cfenv.Service) (string, error) {
	str, ok := service.Credentials["uri"].(string)
	if !ok {
		return "", errors.New("service credentials not available")
	}
	return str, nil

}

func serviceURIByName(env *cfenv.App, serviceName string) (string, error) {
	appEnv, err := cfenv.Current()
	if err != nil {
		return "", err
	}
	service, err := appEnv.Services.WithName(serviceName)
	if err != nil {
		return "", err
	}
	return uriFromService(service)
}
