/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.common;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Class for verifying whether the cluster setup can support
 * all enabled EC policies.
 *
 * Scenarios when the verification fails:
 * 1. not enough data nodes compared to EC policy's highest data+parity number
 * 2. not enough racks to satisfy BlockPlacementPolicyRackFaultTolerant
 * 3. highly uneven racks to satisfy BlockPlacementPolicyRackFaultTolerant
 * 4. highly uneven racks (so that BlockPlacementPolicyRackFaultTolerant's
 * considerLoad logic may exclude some busy nodes on the rack, resulting in #2)
 */
public final class ErasureCodingClusterSetupVerifier {

  public static final Logger LOG =
      LoggerFactory.getLogger(ErasureCodingClusterSetupVerifier.class);

  private ErasureCodingClusterSetupVerifier() {}

  /**
   * Verifies whether the cluster setup can support all enabled EC policies.
   * @return the status of the verification
   * @throws IOException
   */
  public static int getVerifyClusterSetupSupportsEnabledEcPoliciesResult(
      final Set<DatanodeDescriptor> report,
      final ErasureCodingPolicy[] enabledPolicies) {
    int minDN = 0;
    int minRack = 0;
    for (ErasureCodingPolicy policy: enabledPolicies) {
      final int policyDN =
          policy.getNumDataUnits() + policy.getNumParityUnits();
      minDN = Math.max(minDN, policyDN);
      final int policyRack = (int) Math.ceil(
          policyDN / (double) policy.getNumParityUnits());
      minRack = Math.max(minRack, policyRack);
    }
    if (minDN == 0 || minRack == 0) {
      LOG.trace("No erasure coding policy is enabled.");
      return 0;
    }
    return verifyECWithTopology(report, minDN, minRack);
  }

  private static int verifyECWithTopology(final Set<DatanodeDescriptor> report,
                                   final int minDN, final int minRack) {
    if (report.size() < minDN) {
      LOG.debug("The number of DataNodes (" + report.size()
          + ") is less than the minimum required number of DataNodes ("
          + minDN + ") for enabled erasure coding policy.");
      return 1;
    }

    final Map<String, Integer> racks = new HashMap<>();
    for (DatanodeInfo dni : report) {
      Integer count = racks.get(dni.getNetworkLocation());
      if (count == null) {
        count = 0;
      }
      racks.put(dni.getNetworkLocation(), count + 1);
    }

    if (racks.size() < minRack) {
      LOG.debug("The number of racks (" + racks.size()
          + ") is less than the minimum required number of racks ("
          + minRack + ") for enabled erasure coding policy.");
      return 2;
    }

    int minDnPerRack = Integer.MAX_VALUE;
    int maxDnPerRack = 0;
    for (int i : racks.values()) {
      minDnPerRack = Math.min(minDnPerRack, i);
      maxDnPerRack = Math.max(maxDnPerRack, i);
    }
    if ((maxDnPerRack - minDnPerRack) / minDnPerRack >= 1) {
      LOG.debug(
          "The rack configuration is very uneven (minDnPerRack="
              + minDnPerRack + ", maxDnPerRack=" + maxDnPerRack
              + "). This will cause issues to erasure coding block placement."
              + " Reconfigure the topology to more evenly distribute the "
              + " DataNodes among racks.");
      return 3;
    }

    return 0;
  }
}
