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
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.spi.NodeState.ACTIVE;
import static java.util.Objects.requireNonNull;

/**
 * Manage queries scheduled on this node
 */
@Path("/v1/nodeassignment")
public class NodeAssignmentResource
{
  private final NodeTaskMap nodeTaskMap;
  private final NodeManager nodeManager;

  @Inject
  public NodeAssignmentResource(NodeTaskMap nodeTaskMap, NodeManager nodeManager)
  {
    this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
    this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
  }

  @GET
  public List<NodeAssignmentInfo> getNodeAssignments()
  {
    ImmutableList.Builder<NodeAssignmentInfo> nodeAssignments = ImmutableList.builder();
    Set<Node> nodes = nodeManager.getNodes(ACTIVE);

    for (Node node : nodes) {
      HostAddress nodeHostAddress = node.getHostAndPort();
      nodeAssignments.add(new NodeAssignmentInfo(nodeHostAddress.getHostText(), nodeHostAddress.getPort(), nodeTaskMap.getPartitionedSplitsOnNode(node)));
    }
    return nodeAssignments.build();
  }

  @POST
  @Path("blacklist")
  @Consumes(MediaType.APPLICATION_JSON)
  public void setNodeCandidatesBlacklist(NodeAssignmentBlacklistRequest request)
  {
    List<BlacklistNodeInfo> blacklistedNodes = request.getBlacklistedNodes();
    ImmutableList.Builder<HostAddress> blacklistedHostAddresses = ImmutableList.builder();
    for (BlacklistNodeInfo nodeInfo : blacklistedNodes) {
      blacklistedHostAddresses.add(HostAddress.fromParts(nodeInfo.getNodeHost(), nodeInfo.getNodePort()));
    }

    nodeManager.setNodeCandidatesBlacklist(blacklistedHostAddresses.build());
  }
}
