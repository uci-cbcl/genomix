package edu.uci.ics.hyracks.control.nc.work;

import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.control.common.base.IClusterController;
import edu.uci.ics.hyracks.control.common.deployment.DeploymentStatus;
import edu.uci.ics.hyracks.control.common.deployment.DeploymentUtils;
import edu.uci.ics.hyracks.control.common.work.AbstractWork;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;

public class UnDeployBinaryWork extends AbstractWork {

    private DeploymentId deploymentId;
    private NodeControllerService ncs;

    public UnDeployBinaryWork(NodeControllerService ncs, DeploymentId deploymentId) {
        this.deploymentId = deploymentId;
        this.ncs = ncs;
    }

    @Override
    public void run() {
        try {
            DeploymentUtils.undeploy(deploymentId, ncs.getApplicationContext().getJobSerializerDeserializerContainer());
            IClusterController ccs = ncs.getClusterController();
            ccs.notifyDeployBinary(deploymentId, ncs.getId(), DeploymentStatus.SUCCEED);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
