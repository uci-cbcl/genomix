/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.control.cc.work;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.common.deployment.DeploymentRun;
import edu.uci.ics.hyracks.control.common.deployment.DeploymentUtils;
import edu.uci.ics.hyracks.control.common.work.AbstractWork;

public class CliUnDeployBinaryWork extends AbstractWork {

    private ClusterControllerService ccs;
    private DeploymentId deploymentId;

    public CliUnDeployBinaryWork(ClusterControllerService ncs, DeploymentId deploymentId) {
        this.ccs = ncs;
        this.deploymentId = deploymentId;
    }

    @Override
    public void run() {
        try {
            if (deploymentId == null) {
                deploymentId = new DeploymentId(UUID.randomUUID().toString());
            }
            /**
             * Deploy for the cluster controller
             */
            DeploymentUtils.undeploy(deploymentId, ccs.getApplicationContext().getJobSerializerDeserializerContainer(),
                    ccs.getServerContext());

            /**
             * Deploy for the node controllers
             */
            Map<String, NodeControllerState> nodeControllerStateMap = ccs.getNodeMap();

            Set<String> nodeIds = new TreeSet<String>();
            for (String nc : nodeControllerStateMap.keySet()) {
                nodeIds.add(nc);
            }
            DeploymentRun dRun = new DeploymentRun(nodeIds);
            ccs.addDeploymentRun(deploymentId, dRun);

            /***
             * deploy binaries to each node controller
             */
            for (NodeControllerState ncs : nodeControllerStateMap.values()) {
                ncs.getNodeController().undeployBinary(deploymentId);
            }

            /**
             * wait for completion
             */
            dRun.waitForCompletion();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
