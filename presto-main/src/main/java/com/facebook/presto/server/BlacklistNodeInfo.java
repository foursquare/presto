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

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.MoreObjects.toStringHelper;

@Immutable
public class BlacklistNodeInfo
{
  private final String nodeHost;
  private final int nodePort;

  @JsonCreator
  public BlacklistNodeInfo(
    @JsonProperty("nodeHost") String nodeHost,
    @JsonProperty("nodePort") int nodePort)
  {
    this.nodeHost = nodeHost;
    this.nodePort = nodePort;
  }

  @JsonProperty
  public String getNodeHost()
  {
    return nodeHost;
  }

  @JsonProperty
  public int getNodePort()
  {
    return nodePort;
  }

  @Override
  public String toString()
  {
    return toStringHelper(this)
      .add("nodeHost", nodeHost)
      .add("nodePort", nodePort)
      .toString();
  }
}
