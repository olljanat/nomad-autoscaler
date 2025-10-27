// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad-autoscaler/plugins"
	nutanix "github.com/hashicorp/nomad-autoscaler/plugins/builtin/target/nutanix/plugin"
)

func main() {
	plugins.Serve(factory)
}

// factory returns a new instance of the Nutanix VMSS plugin.
func factory(log hclog.Logger) interface{} {
	return nutanix.NewNutanixPlugin(log)
}
