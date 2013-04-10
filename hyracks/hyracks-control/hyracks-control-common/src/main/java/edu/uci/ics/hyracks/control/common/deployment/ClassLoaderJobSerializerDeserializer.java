package edu.uci.ics.hyracks.control.common.deployment;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.IJobSerializerDeserializer;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;

public class ClassLoaderJobSerializerDeserializer implements IJobSerializerDeserializer {

    private URLClassLoader classLoader;

    @Override
    public JobSpecification deserialize(byte[] jsBytes) throws HyracksException {
        try {
            if (classLoader == null) {
                return (JobSpecification) JavaSerializationUtils.deserialize(jsBytes);
            }
            return (JobSpecification) JavaSerializationUtils.deserialize(jsBytes, classLoader);
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }

    @Override
    public byte[] serialize(Serializable jobSpec) throws HyracksException {
        try {
            if (classLoader == null) {
                return JavaSerializationUtils.serialize(jobSpec);
            }
            return JavaSerializationUtils.serialize(jobSpec, classLoader);
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }

    @Override
    public void addClassPathURLs(List<URL> binaryURLs) throws HyracksException {
        Collections.sort(binaryURLs, new Comparator<URL>() {
            @Override
            public int compare(URL o1, URL o2) {
                return o1.getFile().compareTo(o2.getFile());
            }
        });
        try {
            if (classLoader == null) {
                /** crate a new classloader */
                URL[] urls = binaryURLs.toArray(new URL[binaryURLs.size()]);
                classLoader = new URLClassLoader(urls, this.getClass().getClassLoader());
            } else {
                /** add URLs to the existing classloader */
                Object[] urls = binaryURLs.toArray(new URL[binaryURLs.size()]);
                Method method = classLoader.getClass().getDeclaredMethod("addURL", new Class[] { URL.class });
                method.setAccessible(true);
                method.invoke(classLoader, urls);
            }
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }
}
