package edu.uci.ics.hyracks.api.deployment;

import java.io.Serializable;

public class DeploymentId implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String deploymentKey;

    public DeploymentId(String deploymentKey) {
        this.deploymentKey = deploymentKey;
    }

    public int hashCode() {
        return deploymentKey.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof DeploymentId)) {
            return false;
        }
        return ((DeploymentId) o).deploymentKey.equals(deploymentKey);
    }

    @Override
    public String toString() {
        return deploymentKey;
    }
}
