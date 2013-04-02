package edu.uci.ics.hyracks.control.nc.work;

import java.net.URL;
import java.util.List;

import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.control.common.base.IClusterController;
import edu.uci.ics.hyracks.control.common.deployment.DeploymentStatus;
import edu.uci.ics.hyracks.control.common.work.AbstractWork;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;

public class DeployBinaryWork extends AbstractWork {

    private DeploymentId deploymentId;
    private NodeControllerService ncs;
    private List<URL> binaryURLs;

    public DeployBinaryWork(NodeControllerService ncs, DeploymentId deploymentId, List<URL> binaryURLs) {
        this.deploymentId = deploymentId;
        this.ncs = ncs;
        this.binaryURLs = binaryURLs;
    }

    @Override
    public void run() {
        ncs.getApplicationContext();
        // add a new class path
        try {
            IClusterController ccs = ncs.getClusterController();
            ccs.notifyDeployBinary(deploymentId, ncs.getId(), DeploymentStatus.SUCCEED);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
