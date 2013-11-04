package edu.uci.ics.genomix.pregelix.io.message;

public class VALID_MESSAGE {

    public static final byte SOURCE_VERTEX_ID = 1 << 0; // used in superclass: MessageWritable
    // reuse 1st bit, because different message types
    public static final byte NODE = 1 << 1; // used in subclass: PathMergeMessage
    // reuse 1st bit
    public static final byte CREATED_EDGE = 1 << 1; // used in subclass: SplitRepeatMessage
    // reuse 1st bit - for NODE in subclass: BubbleMergeMessage
    public static final byte MAJOR_VERTEX_ID_AND_MAJOR_TO_BUBBLE_EDGETYPE = 1 << 2; // used in subclass: BubbleMergeMessage
    public static final byte MINOR_VERTEX_ID_AND_MINOR_TO_BUBBLE_EDGETYPE = 1 << 3; // used in subclass: BubbleMergeMessage
    public static final byte TOP_COVERAGE_VERTEX_ID = 1 << 4; // used in subclass: BubbleMergeMessage
    // reuse 1st bit
    public static final byte PATH_LIST_AND_EDGETYPE_LIST = 1 << 1; // used in subclass: BFSTraverseMessage
    public static final byte SRC_AND_DEST_READ_HEAD_ORIENTATION = 1 << 2; // used in subclass: BFSTraverseMessage
    public static final byte TARGET_VERTEX_ID = 1 << 3; // used in subclass: BFSTraverseMessage
    public static final byte READ_ID = 1 << 4;
    public static final byte TOTAL_BFS_LENGTH = 1 << 5; // used in subclass: BFSTraverseMessage
    public static final byte SCAFFOLDING_MAP = 1 << 6; // used in subclass: BFSTraverseMessage
    // reuse 1st bit
    public static final byte EDGE_MAP = 1 << 1; // used in subclass: SymmetryCheckerMessage
}
