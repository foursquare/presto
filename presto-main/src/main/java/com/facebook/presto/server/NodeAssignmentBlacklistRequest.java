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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class NodeAssignmentBlacklistRequest
{
  private final List<BlacklistNodeInfo> blacklistedNodes;

  @JsonCreator
  public NodeAssignmentBlacklistRequest(@JsonProperty("blacklistedNodes") List<BlacklistNodeInfo> blacklistedNodes)
  {
    this.blacklistedNodes = requireNonNull(blacklistedNodes, "blacklistedNodes is null");
  }

  @JsonProperty
  public List<BlacklistNodeInfo> getBlacklistedNodes()
  {
    return blacklistedNodes;
  }

  @Override
  public String toString()
  {
    return MoreObjects.toStringHelper(this)
      .add("blacklistedNodes", blacklistedNodes)
      .toString();
  }
}
