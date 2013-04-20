package edu.uci.ics.hyracks.control.common.deployment;

import java.util.Set;
import java.util.TreeSet;

public class DeploymentRun implements IDeploymentStatusConditionVariable {

    private DeploymentStatus deploymentStatus = DeploymentStatus.FAIL;
    private final Set<String> deploymentNodeIds = new TreeSet<String>();

    public DeploymentRun(Set<String> nodeIds) {
        deploymentNodeIds.addAll(nodeIds);
    }

    public synchronized void notifyDeploymentStatus(String nodeId, DeploymentStatus status) {
        deploymentNodeIds.remove(nodeId);
        if (deploymentNodeIds.size() == 0) {
            deploymentStatus = DeploymentStatus.SUCCEED;
            notifyAll();
        }
    }

    @Override
    public synchronized DeploymentStatus waitForCompletion() throws Exception {
        wait();
        return deploymentStatus;
    }

}
