/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.server;

import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.facebook.presto.spi.NodeState.SHUTTING_DOWN;
import static java.util.Objects.requireNonNull;

/**
 * Manage queries scheduled on this node
 */
@Path("/v1/nodeassignment")
public class NodeAssignmentResource
{
  private final NodeTaskMap nodeTaskMap;
  private final InternalNodeManager nodeManager;

  @Inject
  public NodeAssignmentResource(NodeTaskMap nodeTaskMap, InternalNodeManager nodeManager)
  {
    this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
    this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
  }

  @GET
  public List<NodeAssignmentInfo> getNodeAssignments()
  {
    ImmutableList.Builder<NodeAssignmentInfo> nodeAssignments = ImmutableList.builder();
    Set<Node> activeNodes = nodeManager.getNodes(ACTIVE);
    Set<Node> shutdownNodes = nodeManager.getNodes(SHUTTING_DOWN);

    for (Node node : activeNodes) {
      HostAddress nodeHostAddress = node.getHostAndPort();
      nodeAssignments.add(new NodeAssignmentInfo(
        nodeHostAddress.getHostText(),
        nodeHostAddress.getPort(),
        nodeTaskMap.getPartitionedSplitsOnNode(node),
        nodeTaskMap.getNumberOfTasksOnNode(node),
        "ACTIVE"
      ));
    }
    for (Node node : shutdownNodes) {
      HostAddress nodeHostAddress = node.getHostAndPort();
      nodeAssignments.add(new NodeAssignmentInfo(
        nodeHostAddress.getHostText(),
        nodeHostAddress.getPort(),
        nodeTaskMap.getPartitionedSplitsOnNode(node),
        nodeTaskMap.getNumberOfTasksOnNode(node),
        "SHUTDOWN"
      ));
    }
    return nodeAssignments.build();
  }
}
