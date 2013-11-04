package edu.uci.ics.genomix.pregelix.io.message;

public class VALID_MESSAGE {
    
    private final byte SOURCE_VERTEX_ID = 1 << 1; // used in superclass: MessageWritable
    
    // reuse 1st bit, because different message type
    private final byte NODE = 1 << 2; // used in subclass: PathMergeMessage
    // reuse 1st bit
    private final byte CREATED_EDGE = 1 << 2; // used in subclass: SplitRepeatMessage
    // reuse 1st bit
    private final byte MAJOR_VERTEX_ID = 1 << 2; // used in subclass: BubbleMergeMessage
    private final byte MINOR_VERTEX_ID = 1 << 3; // used in subclass: BubbleMergeMessage
    private final byte MAJOR_TO_BUBBLE_EDGETYPE = 1 << 4; // used in subclass: BubbleMergeMessage
    private final byte MINOR_TO_BUBBLE_EDGETYPE = 1 << 5; // used in subclass: BubbleMergeMessage
    private final byte TOP_COVERAGE_VERTEX_ID = 1 << 6; // used in subclass: BubbleMergeMessage
    
    private final byte PATH_LIST = 1 << 2; // used in subclass: BFSTraverseMessage
    private final byte EDGETYPE_LIST = 1 << 3; // used in subclass: BFSTraverseMessage
    private final byte TARGET_VERTEX_ID = 1 << 4; // used in subclass: BFSTraverseMessage
    private final byte TOTAL_BFS_LENGTH = 1 << 5; // used in subclass: BFSTraverseMessage
    private final byte SCAFFOLDING_MAP = 1 << 6; // used in subclass: BFSTraverseMessage
    
    private final byte EDGE_MAP = 1 << 2; // used in subclass: SymmetryCheckerMessage
}
