package com.hiwuyue.dag;

import java.util.Map;
import java.util.Set;

public interface DagGraph {
    boolean validateAcyclic();

    Set<DagNode> getDagNodes();

    Map<DagNode,Set<DagNode>> getDagNodeDependencies();

    void addNode(DagNode node);

    void addEdge(DagNode from, DagNode to);
}
