package com.hiwuyue.dag;

import java.util.Map;
import java.util.Set;

public interface DagGraph {
    String getName();

    boolean validateAcyclic();

    Set<DagNode> getDagNodes();

    Map<DagNode, Set<DagNode>> getDagNodeDependencies();

    void addNode(DagNode node);

    void removeNode(DagNode node);

    void addEdge(DagNode from, DagNode to);

    void removeEdge(DagNode from, DagNode to);
}
