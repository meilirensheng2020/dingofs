/*
 *  Copyright (c) 2022 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: CurveCli
 * Created Date: 2022-06-25
 * Author: chengyi (Cyber-SiKu)
 */

package cluster

import (
	"fmt"

	"github.com/olekukonko/tablewriter"
	cmderror "github.com/dingodb/dingofs/tools-v2/internal/error"
	cobrautil "github.com/dingodb/dingofs/tools-v2/internal/utils"
	basecmd "github.com/dingodb/dingofs/tools-v2/pkg/cli/command"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/status/copyset"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/status/etcd"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/status/mds"
	"github.com/dingodb/dingofs/tools-v2/pkg/cli/command/status/metaserver"
	"github.com/dingodb/dingofs/tools-v2/pkg/config"
	"github.com/dingodb/dingofs/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

const (
	TYPE_ETCD        = "etcd"
	TYPE_MDS         = "mds"
	TYPE_MEATASERVER = "meataserver"
	TYPE_COPYSET     = "copyset"
)

type ClusterCommand struct {
	basecmd.FinalCurveCmd
	type2Table map[string]*tablewriter.Table
	type2Func  map[string]func(caller *cobra.Command) (*interface{}, *tablewriter.Table, *cmderror.CmdError, cobrautil.ClUSTER_HEALTH_STATUS)
	serverList []string
	health     cobrautil.ClUSTER_HEALTH_STATUS
}

var _ basecmd.FinalCurveCmdFunc = (*ClusterCommand)(nil) // check interface

const (
	clusterExample = `$ curve status cluster`
)

func NewClusterCommand() *cobra.Command {
	cCmd := &ClusterCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "cluster",
			Short:   "get status of the curvefs",
			Example: clusterExample,
		},
	}
	basecmd.NewFinalCurveCli(&cCmd.FinalCurveCmd, cCmd)
	return cCmd.Cmd
}

func (cCmd *ClusterCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(cCmd.Cmd)
	config.AddRpcTimeoutFlag(cCmd.Cmd)
	config.AddFsMdsAddrFlag(cCmd.Cmd)
	config.AddFsMdsDummyAddrFlag(cCmd.Cmd)
	config.AddEtcdAddrFlag(cCmd.Cmd)
}

func (cCmd *ClusterCommand) Init(cmd *cobra.Command, args []string) error {
	cCmd.type2Func = map[string]func(caller *cobra.Command) (*interface{}, *tablewriter.Table, *cmderror.CmdError, cobrautil.ClUSTER_HEALTH_STATUS){
		TYPE_ETCD:        etcd.GetEtcdStatus,
		TYPE_MDS:         mds.GetMdsStatus,
		TYPE_MEATASERVER: metaserver.GetMetaserverStatus,
		TYPE_COPYSET:     copyset.GetCopysetStatus,
	}
	cCmd.type2Table = make(map[string]*tablewriter.Table)
	cCmd.serverList = []string{TYPE_ETCD, TYPE_MDS, TYPE_MEATASERVER, TYPE_COPYSET}
	cCmd.health = cobrautil.HEALTH_OK
	return nil
}

func (cCmd *ClusterCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cCmd.FinalCurveCmd, cCmd)
}

func (cCmd *ClusterCommand) RunCommand(cmd *cobra.Command, args []string) error {
	var errs []*cmderror.CmdError
	results := make(map[string]interface{})
	for key, function := range cCmd.type2Func {
		result, table, err, health := function(cmd)
		cCmd.type2Table[key] = table
		results[key] = *result
		errs = append(errs, err)
		cCmd.health = cobrautil.CompareHealth(cCmd.health, health)
	}
	finalErr := cmderror.MergeCmdErrorExceptSuccess(errs)
	cCmd.Error = finalErr
	results["health"] = cobrautil.ClusterHealthStatus_Str[int32(cCmd.health)]
	cCmd.Result = results
	return nil
}

func (cCmd *ClusterCommand) ResultPlainOutput() error {
	for _, server := range cCmd.serverList {
		fmt.Printf("%s:\n", server)
		if cCmd.type2Table[server] != nil && cCmd.type2Table[server].NumLines() > 0 {
			cCmd.type2Table[server].Render()
		} else {
			fmt.Printf("No found %s\n", server)
		}
	}
	fmt.Println("Cluster health is:", cobrautil.ClusterHealthStatus_Str[int32(cCmd.health)])
	return nil
}
