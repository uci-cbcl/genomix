package edu.uci.ics.hyracks.control.common.deployment;

import java.net.URL;
import java.util.List;

import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.IJobSerializerDeserializer;
import edu.uci.ics.hyracks.api.job.IJobSerializerDeserializerContainer;

public class DeploymentUtils {

    public static void deploy(DeploymentId deploymentId, List<URL> urls, IJobSerializerDeserializerContainer container)
            throws HyracksException {
        IJobSerializerDeserializer jobSerDe = container.getJobSerializerDeerializer(deploymentId);
        if (jobSerDe == null) {
            jobSerDe = new ClassLoaderJobSerializerDeserializer();
            container.addJobSerializerDeserializer(deploymentId, jobSerDe);
        }
        jobSerDe.addClassPathURLs(urls);
    }

}
