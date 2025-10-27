// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad-autoscaler/plugins"
	"github.com/hashicorp/nomad-autoscaler/plugins/base"
	"github.com/hashicorp/nomad-autoscaler/plugins/target"
	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/hashicorp/nomad/api"
	prismapi "github.com/nutanix/ntnx-api-golang-clients/prism-go-client/v4/api"
	prismclient "github.com/nutanix/ntnx-api-golang-clients/prism-go-client/v4/client"
	prismconfig "github.com/nutanix/ntnx-api-golang-clients/prism-go-client/v4/models/prism/v4/config"
	vmmapi "github.com/nutanix/ntnx-api-golang-clients/vmm-go-client/v4/api"
	vmmclient "github.com/nutanix/ntnx-api-golang-clients/vmm-go-client/v4/client"
	ahvconfig "github.com/nutanix/ntnx-api-golang-clients/vmm-go-client/v4/models/vmm/v4/ahv/config"
	vmmerror "github.com/nutanix/ntnx-api-golang-clients/vmm-go-client/v4/models/vmm/v4/error"
)

const (
	pluginName           = "nutanix"
	nodeAttrVMUUID       = "unique.platform.nutanix.vm-uuid"
	defaultRetryAttempts = 5
	defaultRetryInterval = 5 * time.Second
	defaultTaskTimeout   = 300 * time.Second
	defaultPollInterval  = 5 * time.Second
)

var (
	PluginConfig = &plugins.InternalPluginConfig{
		Factory: func(l hclog.Logger) interface{} { return NewNutanixPlugin(l) },
	}

	pluginInfo = &base.PluginInfo{
		Name:       pluginName,
		PluginType: sdk.PluginTypeTarget,
	}
)

// Assert that TargetPlugin meets the target.Target interface.
var _ target.Target = (*TargetPlugin)(nil)

// TargetPlugin is the Nutanix implementation of the target.Target interface.
type TargetPlugin struct {
	logger        hclog.Logger
	vmmClient     *vmmclient.ApiClient
	prismClient   *prismclient.ApiClient
	endpoint      string
	username      string
	password      string
	insecure      bool
	clusterUUID   string
	imageUUID     string
	subnetUUID    string
	categoryKey   string
	categoryValue string
}

func NewNutanixPlugin(log hclog.Logger) *TargetPlugin {
	return &TargetPlugin{
		logger: log,
	}
}

func (t *TargetPlugin) SetConfig(config map[string]string) error {
	var err error

	t.endpoint = config["endpoint"]
	if t.endpoint == "" {
		return fmt.Errorf("required config 'endpoint' not set")
	}

	t.username = config["username"]
	if t.username == "" {
		return fmt.Errorf("required config 'username' not set")
	}

	t.password = config["password"]
	if t.password == "" {
		return fmt.Errorf("required config 'password' not set")
	}

	insecureStr := config["insecure"]
	if insecureStr != "" {
		t.insecure, err = strconv.ParseBool(insecureStr)
		if err != nil {
			return fmt.Errorf("invalid 'insecure' value: %v", err)
		}
	} else {
		t.insecure = true // default to true for self-signed certs
	}

	t.clusterUUID = config["cluster_uuid"]
	if t.clusterUUID == "" {
		return fmt.Errorf("required config 'cluster_uuid' not set")
	}

	t.imageUUID = config["image_uuid"]
	if t.imageUUID == "" {
		return fmt.Errorf("required config 'image_uuid' not set")
	}

	t.subnetUUID = config["subnet_uuid"]
	if t.subnetUUID == "" {
		return fmt.Errorf("required config 'subnet_uuid' not set")
	}

	t.categoryKey = config["category_key"]
	if t.categoryKey == "" {
		t.categoryKey = "nomad_pool"
	}

	t.categoryValue = config["category_value"]
	if t.categoryValue == "" {
		t.categoryValue = "default"
	}

	// Initialize the VMM client
	t.vmmClient = vmmclient.NewApiClient()
	t.vmmClient.Host = t.endpoint
	t.vmmClient.Port = 9440
	t.vmmClient.Username = t.username
	t.vmmClient.Password = t.password
	t.vmmClient.VerifySSL = !t.insecure
	t.vmmClient.RetryInterval = 100
	t.vmmClient.MaxRetryAttempts = 2
	t.vmmClient.Debug = false

	// Initialize the Prism client for tasks
	t.prismClient = prismclient.NewApiClient()
	t.prismClient.Host = t.endpoint
	t.prismClient.Port = 9440
	t.prismClient.Username = t.username
	t.prismClient.Password = t.password
	t.prismClient.VerifySSL = !t.insecure
	t.prismClient.RetryInterval = 100
	t.prismClient.MaxRetryAttempts = 2
	t.prismClient.Debug = false

	return nil
}

// PluginInfo satisfies the PluginInfo function on the base.Base interface.
func (t *TargetPlugin) PluginInfo() (*base.PluginInfo, error) {
	return pluginInfo, nil
}

func (t *TargetPlugin) Scale(action target.ScalingAction, config map[string]string) error {
	ctx := context.Background()

	status := t.Status(nil)
	if !status.Ready {
		return fmt.Errorf("target not ready: %s", status.Message)
	}

	current := status.Count
	diff := action.Count - current

	if diff == 0 {
		return nil
	}

	vmApi := vmmapi.NewVmApi(t.vmmClient)

	if diff > 0 {
		// Scale out: create new VMs
		for i := int64(0); i < diff; i++ {
			vmName := fmt.Sprintf("nomad-client-%d-%d", time.Now().Unix(), rand.Intn(1000))

			clusterRef := ahvconfig.ClusterReference{ExtId: &t.clusterUUID}
			subnetRef := ahvconfig.SubnetReference{ExtId: &t.subnetUUID}
			imageRef := ahvconfig.ImageReference{ImageExtId: &t.imageUUID}

			numSockets := int64(1)
			numCores := int64(2)
			memoryBytes := int64(4096 * 1024 * 1024)
			diskSizeMib := int64(51200)

			vm := ahvconfig.Vm{
				Name:              &vmName,
				NumSockets:        &numSockets,
				NumCoresPerSocket: &numCores,
				MemorySizeBytes:   &memoryBytes,
				Cluster:           &clusterRef,
				Nics: []*ahvconfig.Nic{
					{
						NetworkInfo: &ahvconfig.NicNetworkInfo{
							NicType: ahvconfig.NicType.NORMAL_NIC.Ptr(),
							Subnet:  &subnetRef,
							Ipv4Config: &ahvconfig.Ipv4Config{
								ShouldAssignIp: true.Ptr(),
							},
						},
						BackingInfo: &ahvconfig.EmulatedNic{
							Model:       ahvconfig.EmulatedNicModel.VIRTIO.Ptr(),
							IsConnected: true.Ptr(),
						},
					},
				},
				Disks: []*ahvconfig.Disk{
					{
						BackingInfo: &ahvconfig.VmDisk{
							DataSource: &ahvconfig.DataSource{
								Reference: &imageRef,
							},
							DiskSizeMib: &diskSizeMib,
						},
						DiskAddress: &ahvconfig.DiskAddress{
							BusType: ahvconfig.DiskBusType.SCSI.Ptr(),
							Index:   int64(0).Ptr(),
						},
					},
				},
				Categories: map[string]string{
					t.categoryKey: t.categoryValue,
				},
			}

			createResp, err := vmApi.CreateVm(ctx, &vm)
			if err != nil {
				return fmt.Errorf("failed to create VM: %v", err)
			}

			data := createResp.GetData()
			createResult, ok := data.(*ahvconfig.Vm)
			if !ok {
				errResp := data.(*vmmerror.ErrorResponse)
				return fmt.Errorf("failed to create VM: %v", errResp.Error())
			}

			taskUUID := *createResult.ExtId // This is the task UUID

			err = t.monitorTask(ctx, taskUUID)
			if err != nil {
				return fmt.Errorf("VM creation task failed: %v", err)
			}

			// Get the VM UUID from the task
			vmUUID, err := t.getEntityFromTask(ctx, taskUUID)
			if err != nil {
				return fmt.Errorf("failed to get VM UUID from task: %v", err)
			}

			// Power on the VM
			err = t.powerOnVM(ctx, vmUUID)
			if err != nil {
				return fmt.Errorf("failed to power on VM %s: %v", vmUUID, err)
			}
		}
	} else {
		// Scale in: delete specific VMs corresponding to drained nodes
		nodeIDsStr, ok := config["node_ids"]
		if !ok || nodeIDsStr == "" {
			return fmt.Errorf("no 'node_ids' provided for scale in")
		}

		nodeIDs := strings.Split(nodeIDsStr, ",")
		numToDelete := int(-diff)
		if len(nodeIDs) != numToDelete {
			return fmt.Errorf("expected %d node_ids, got %d", numToDelete, len(nodeIDs))
		}

		nomadClient, err := api.NewClient(api.DefaultConfig())
		if err != nil {
			return fmt.Errorf("failed to create Nomad client: %v", err)
		}

		for _, nodeID := range nodeIDs {
			node, _, err := nomadClient.Nodes().Info(nodeID, nil)
			if err != nil {
				return fmt.Errorf("failed to get Nomad node %s: %v", nodeID, err)
			}

			vmUUID, ok := node.Attributes[nodeAttrVMUUID]
			if !ok {
				return fmt.Errorf("VM UUID attribute '%s' not found for node %s", nodeAttrVMUUID, nodeID)
			}

			deleteResp, err := vmApi.DeleteVmById(ctx, vmUUID)
			if err != nil {
				return fmt.Errorf("failed to delete VM %s: %v", vmUUID, err)
			}

			data := deleteResp.GetData()
			deleteResult, ok := data.(*ahvconfig.Vm)
			if !ok {
				errResp := data.(*vmmerror.ErrorResponse)
				return fmt.Errorf("failed to delete VM: %v", errResp.Error())
			}

			taskUUID := *deleteResult.ExtId

			err = t.monitorTask(ctx, taskUUID)
			if err != nil {
				return fmt.Errorf("VM deletion task failed: %v", err)
			}
		}
	}

	// Wait for the cluster to stabilize
	err := t.waitForCount(ctx, action.Count)
	if err != nil {
		return fmt.Errorf("failed to reach desired count: %v", err)
	}

	return nil
}

// Status satisfies the Status function on the target.Target interface.
func (t *TargetPlugin) Status(config map[string]string) (*sdk.TargetStatus, error) {
	ctx := context.Background()

	offset := int64(0)
	limit := int64(1000) // Adjust as needed
	filter := fmt.Sprintf("cluster==%s;categories.get('%s')==%s", t.clusterUUID, t.categoryKey, t.categoryValue)
	orderBy := ""
	_select := ""

	vmApi := vmmapi.NewVmApi(t.vmmClient)
	response, err := vmApi.ListVms(ctx, &offset, &limit, &filter, &orderBy, &_select)
	if err != nil {
		return &target.TargetStatus{Ready: false, Count: 0, Meta: nil, Message: err.Error()}
	}

	data := response.GetData()
	resultVms, ok := data.([]ahvconfig.Vm)
	if !ok {
		errResp := data.(*vmmerror.ErrorResponse)
		return &target.TargetStatus{Ready: false, Count: 0, Meta: nil, Message: errResp.Error()}
	}

	count := int64(len(resultVms))
	ready := true
	for _, vm := range resultVms {
		if *vm.PowerState != "ON" {
			ready = false
			break
		}
	}

	return &target.TargetStatus{Ready: ready, Count: count, Meta: nil, Message: ""}
}

func (t *TargetPlugin) monitorTask(ctx context.Context, taskUUID string) error {
	tasksApi := prismapi.NewTasksApi(t.prismClient)
	startTime := time.Now()

	for {
		if time.Since(startTime) > defaultTaskTimeout {
			return fmt.Errorf("task %s timed out", taskUUID)
		}

		response, err := tasksApi.GetTaskById(ctx, taskUUID)
		if err != nil {
			return fmt.Errorf("failed to get task %s: %v", taskUUID, err)
		}

		data := response.GetData()
		task, ok := data.(*prismconfig.Task)
		if !ok {
			errResp := data.(*vmmerror.ErrorResponse) // Reuse vmmerror, adjust if different
			return fmt.Errorf("failed to get task: %v", errResp.Error())
		}

		if *task.ProgressStatus == "SUCCEEDED" {
			return nil
		} else if *task.ProgressStatus == "FAILED" {
			return fmt.Errorf("task failed: %s", *task.FailureReason)
		}

		time.Sleep(defaultPollInterval)
	}
}

func (t *TargetPlugin) getEntityFromTask(ctx context.Context, taskUUID string) (string, error) {
	tasksApi := prismapi.NewTasksApi(t.prismClient)
	response, err := tasksApi.GetTaskById(ctx, taskUUID)
	if err != nil {
		return "", fmt.Errorf("failed to get task %s: %v", taskUUID, err)
	}

	data := response.GetData()
	task, ok := data.(*prismconfig.Task)
	if !ok {
		errResp := data.(*vmmerror.ErrorResponse)
		return "", fmt.Errorf("failed to get task: %v", errResp.Error())
	}

	if len(task.EntitiesAffected) == 0 {
		return "", fmt.Errorf("no entities affected in task %s", taskUUID)
	}

	return *task.EntitiesAffected[0].ExtId, nil
}

func (t *TargetPlugin) powerOnVM(ctx context.Context, vmUUID string) error {
	vmApi := vmmapi.NewVmApi(t.vmmClient)

	// Get the current VM to retrieve ETag
	getResp, err := vmApi.GetVmById(ctx, vmUUID)
	if err != nil {
		return fmt.Errorf("failed to get VM %s: %v", vmUUID, err)
	}

	data := getResp.GetData()
	currentVm, ok := data.(*ahvconfig.Vm)
	if !ok {
		errResp := data.(*vmmerror.ErrorResponse)
		return fmt.Errorf("failed to get VM: %v", errResp.Error())
	}

	etag := t.vmmClient.GetEtag(getResp) // Assuming GetEtag method exists, similar to Python

	powerState := "ON"
	updateVm := ahvconfig.Vm{
		PowerState: &powerState,
	}

	headers := map[string]string{"If-Match": etag}

	// Update with headers
	updateResp, err := vmApi.UpdateVmByIdWithHeaders(ctx, vmUUID, &updateVm, headers)
	if err != nil {
		return fmt.Errorf("failed to update VM %s: %v", vmUUID, err)
	}

	data = updateResp.GetData()
	updateResult, ok := data.(*ahvconfig.Vm)
	if !ok {
		errResp := data.(*vmmerror.ErrorResponse)
		return fmt.Errorf("failed to update VM: %v", errResp.Error())
	}

	taskUUID := *updateResult.ExtId

	err = t.monitorTask(ctx, taskUUID)
	if err != nil {
		return fmt.Errorf("VM power on task failed: %v", err)
	}

	return nil
}

// waitForCount retries until the VM count matches the desired count
func (t *TargetPlugin) waitForCount(ctx context.Context, desired int64) error {
	f := func(ctx context.Context) (bool, error) {
		status := t.Status(nil)
		if status.Count == desired && status.Ready {
			return true, nil
		}
		return false, fmt.Errorf("current count %d (ready: %v), desired %d", status.Count, status.Ready, desired)
	}

	return retry(ctx, defaultRetryInterval, defaultRetryAttempts, f)
}

// retry is a simple retry function
func retry(ctx context.Context, interval time.Duration, attempts int, f func(context.Context) (bool, error)) error {
	for i := 0; i < attempts; i++ {
		done, err := f(ctx)
		if done {
			return err
		}
		time.Sleep(interval)
	}
	return fmt.Errorf("retry attempts exhausted")
}
