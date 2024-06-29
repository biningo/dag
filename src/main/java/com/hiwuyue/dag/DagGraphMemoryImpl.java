package com.hiwuyue.dag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DagGraphMemoryImpl implements DagGraph {
    private Set<DagNode> dagNodes;

    private Map<DagNode, Set<DagNode>> dagNodeDependencies;

    @Override
    public boolean validateAcyclic() {
        HashMap<DagNode, Integer> degree = new HashMap<>();
        dagNodeDependencies.forEach((node, prevNodes) -> {
            degree.put(node, prevNodes.size());
        });

        LinkedList<DagNode> inDepNodes = new LinkedList<>();
        degree.forEach((node, edgeCount) -> {
            if (edgeCount == 0) {
                inDepNodes.addLast(node);
            }
        });

        HashMap<DagNode, Set<DagNode>> nextNodes = new HashMap<>();
        dagNodeDependencies.forEach((node, prevNodes) -> {
            for (DagNode prevNode : prevNodes) {
                nextNodes.putIfAbsent(prevNode, new HashSet<>());
                nextNodes.get(prevNode).add(node);
            }
        });

        List<DagNode> removedNodes = new ArrayList<>();
        while (!inDepNodes.isEmpty()) {
            DagNode node = inDepNodes.removeFirst();
            removedNodes.add(node);

            Set<DagNode> nodes = nextNodes.get(node);
            for (DagNode nextNode : nodes) {
                int edgeCount = degree.get(nextNode) - 1;
                degree.put(nextNode, edgeCount);
                if (edgeCount == 0) {
                    inDepNodes.addLast(nextNode);
                }
            }
        }

        return removedNodes.size() == this.dagNodes.size();
    }

    @Override
    public Set<DagNode> getDagNodes() {
        return dagNodes;
    }

    @Override
    public Map<DagNode, Set<DagNode>> getDagNodeDependencies() {
        return dagNodeDependencies;
    }

    @Override
    public void addNode(DagNode node) {
        this.dagNodes.add(node);
    }

    @Override
    public void addEdge(DagNode from, DagNode to) {
        this.dagNodeDependencies.putIfAbsent(to, new HashSet<>());
        this.dagNodeDependencies.get(to).add(from);
    }
}
