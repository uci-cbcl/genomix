package edu.uci.ics.hyracks.control.common.deployment;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;

import edu.uci.ics.hyracks.api.application.IApplicationContext;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.IJobSerializerDeserializer;
import edu.uci.ics.hyracks.api.job.IJobSerializerDeserializerContainer;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.control.common.context.ServerContext;

public class DeploymentUtils {

    private static final String DEPLOYMENT = "deployment";

    public static void undeploy(DeploymentId deploymentId, IJobSerializerDeserializerContainer container)
            throws HyracksException {
        container.removeJobSerializerDeserializer(deploymentId);
    }

    public static void deploy(DeploymentId deploymentId, List<URL> urls, IJobSerializerDeserializerContainer container,
            ServerContext ctx) throws HyracksException {
        IJobSerializerDeserializer jobSerDe = container.getJobSerializerDeerializer(deploymentId);
        if (jobSerDe == null) {
            jobSerDe = new ClassLoaderJobSerializerDeserializer();
            container.addJobSerializerDeserializer(deploymentId, jobSerDe);
        }
        String rootDir = ctx.getBaseDir().toString();
        String deploymentDir = rootDir.endsWith(File.separator) ? rootDir + DEPLOYMENT : rootDir + File.separator
                + DEPLOYMENT;
        jobSerDe.addClassPathURLs(downloadURLs(urls, deploymentDir));
    }

    public static Object deserialize(byte[] bytes, DeploymentId deploymentId, IApplicationContext appCtx)
            throws HyracksException {
        try {
            IJobSerializerDeserializerContainer jobSerDeContainer = appCtx.getJobSerializerDeserializerContainer();
            IJobSerializerDeserializer jobSerDe = deploymentId == null ? null : jobSerDeContainer
                    .getJobSerializerDeerializer(deploymentId);
            Object obj = jobSerDe == null ? JavaSerializationUtils.deserialize(bytes) : jobSerDe.deserialize(bytes);
            return obj;
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }

    private static List<URL> downloadURLs(List<URL> urls, String deploymentDir) throws HyracksException {
        try {
            List<URL> downloadedFileURLs = new ArrayList<URL>();
            File dir = new File(deploymentDir);
            if (!dir.exists()) {
                FileUtils.forceMkdir(dir);
            }
            for (URL url : urls) {
                String urlString = url.toString();
                int slashIndex = urlString.lastIndexOf('/');
                String fileName = urlString.substring(slashIndex + 1);
                String filePath = deploymentDir + File.separator + fileName;
                File targetFile = new File(filePath);
                FileUtils.copyURLToFile(url, targetFile);
                downloadedFileURLs.add(targetFile.toURI().toURL());
            }
            return downloadedFileURLs;
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }
}
