// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"context"
	basecmd "github.com/dingodb/dingofs/tools-v2/pkg/cli/command"
	"github.com/dingodb/dingofs/tools-v2/pkg/output"
	"github.com/dingodb/dingofs/tools-v2/proto/curvefs/proto/metaserver"
	"github.com/dingodb/dingofs/tools-v2/proto/curvefs/proto/topology"
	"google.golang.org/grpc"
)

type GetFsQuotaRpc struct {
	Info             *basecmd.Rpc
	Request          *metaserver.GetFsQuotaRequest
	metaServerClient metaserver.MetaServerServiceClient
}

type SetFsQuotaRpc struct {
	Info             *basecmd.Rpc
	Request          *metaserver.SetFsQuotaRequest
	metaServerClient metaserver.MetaServerServiceClient
}

type QueryCopysetRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.GetCopysetsInfoRequest
	topologyClient topology.TopologyServiceClient
}

type ListPartitionRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListPartitionRequest
	topologyClient topology.TopologyServiceClient
}

type GetInodeAttrRpc struct {
	Info             *basecmd.Rpc
	Request          *metaserver.BatchGetInodeAttrRequest
	metaServerClient metaserver.MetaServerServiceClient
}

type ListDentryRpc struct {
	Info             *basecmd.Rpc
	Request          *metaserver.ListDentryRequest
	metaServerClient metaserver.MetaServerServiceClient
}

var _ basecmd.RpcFunc = (*GetFsQuotaRpc)(nil)    // check interface
var _ basecmd.RpcFunc = (*SetFsQuotaRpc)(nil)    // check interface
var _ basecmd.RpcFunc = (*QueryCopysetRpc)(nil)  // check interface
var _ basecmd.RpcFunc = (*ListPartitionRpc)(nil) // check interface
var _ basecmd.RpcFunc = (*GetInodeAttrRpc)(nil)  // check interface
var _ basecmd.RpcFunc = (*ListDentryRpc)(nil)    // check interface

func (getFsQuotaRpc *GetFsQuotaRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	getFsQuotaRpc.metaServerClient = metaserver.NewMetaServerServiceClient(cc)
}

func (getFsQuotaRpc *GetFsQuotaRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := getFsQuotaRpc.metaServerClient.GetFsQuota(ctx, getFsQuotaRpc.Request)
	output.ShowRpcData(getFsQuotaRpc.Request, response, getFsQuotaRpc.Info.RpcDataShow)
	return response, err
}

func (setFsQuotaRpc *SetFsQuotaRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	setFsQuotaRpc.metaServerClient = metaserver.NewMetaServerServiceClient(cc)
}

func (setFsQuotaRpc *SetFsQuotaRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := setFsQuotaRpc.metaServerClient.SetFsQuota(ctx, setFsQuotaRpc.Request)
	output.ShowRpcData(setFsQuotaRpc.Request, response, setFsQuotaRpc.Info.RpcDataShow)
	return response, err
}

func (qcRpc *QueryCopysetRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	qcRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (qcRpc *QueryCopysetRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := qcRpc.topologyClient.GetCopysetsInfo(ctx, qcRpc.Request)
	output.ShowRpcData(qcRpc.Request, response, qcRpc.Info.RpcDataShow)
	return response, err
}

func (lpRp *ListPartitionRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lpRp.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (lpRp *ListPartitionRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := lpRp.topologyClient.ListPartition(ctx, lpRp.Request)
	output.ShowRpcData(lpRp.Request, response, lpRp.Info.RpcDataShow)
	return response, err
}

func (getInodeRpc *GetInodeAttrRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	getInodeRpc.metaServerClient = metaserver.NewMetaServerServiceClient(cc)
}

func (getInodeRpc *GetInodeAttrRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := getInodeRpc.metaServerClient.BatchGetInodeAttr(ctx, getInodeRpc.Request)
	output.ShowRpcData(getInodeRpc.Request, response, getInodeRpc.Info.RpcDataShow)
	return response, err
}

func (listDentryRpc *ListDentryRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	listDentryRpc.metaServerClient = metaserver.NewMetaServerServiceClient(cc)
}

func (listDentryRpc *ListDentryRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := listDentryRpc.metaServerClient.ListDentry(ctx, listDentryRpc.Request)
	output.ShowRpcData(listDentryRpc.Request, response, listDentryRpc.Info.RpcDataShow)
	return response, err
}
