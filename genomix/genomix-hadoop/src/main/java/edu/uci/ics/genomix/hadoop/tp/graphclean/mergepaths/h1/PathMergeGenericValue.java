package edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h1;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;


@SuppressWarnings("unchecked")
public class PathMergeGenericValue extends GenericWritable  {

    private static Class<? extends Writable>[] CLASSES = null;

    static {
        CLASSES = (Class<? extends Writable>[]) new Class[] {
            PathMergeNode.class,
            PathMergeMsgWritable.class
        };
    }

    //this empty initialize is required by hadoop
    public PathMergeGenericValue() {
    }

    public PathMergeGenericValue(Writable instance) {
        set(instance);
    }

    @Override
    protected Class<? extends Writable>[] getTypes() {
        return CLASSES;
    }
    
    @Override
    public String toString() {
        return get().toString();
    }
}
