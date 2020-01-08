/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class TestNodeLabelsUpdater {

  DriverApplicationMaster.NodeLabelsUpdater nodeLabelsUpdater;

  @BeforeMethod
  public void setup() {
    nodeLabelsUpdater = new DriverApplicationMaster().new NodeLabelsUpdater();
  }

  @Captor ArgumentCaptor<AddToClusterNodeLabelsRequest> addLabelsArg = ArgumentCaptor.forClass(AddToClusterNodeLabelsRequest.class);
  @Captor ArgumentCaptor<ReplaceLabelsOnNodeRequest> replaceLabelsArg = ArgumentCaptor.forClass(ReplaceLabelsOnNodeRequest.class);
  @Test
  public void testUpdateNodeLabels() throws Exception {
    ResourceManagerAdministrationProtocol rmAdminClient = mock(ResourceManagerAdministrationProtocol.class);
    Map<String, Integer> labelToCounts = new HashMap<>();
    labelToCounts.put("label1", 100);
    labelToCounts.put("label2", 150);
    List<NodeReport> nodeReports = new ArrayList<>();
    for (int i = 0; i < 250; i++) {
      NodeReport report = mock(NodeReport.class);
      when(report.getNodeId()).thenReturn(NodeId.newInstance("host", i));
      nodeReports.add(report);
    }
    nodeLabelsUpdater.remoteAddLabels(rmAdminClient, labelToCounts, nodeReports);
    verify(rmAdminClient).addToClusterNodeLabels(addLabelsArg.capture());
    List<NodeLabel> labels = addLabelsArg.getValue().getNodeLabels();
    assertEquals(labels.size(), 2);
    assertTrue(labels.contains(NodeLabel.newInstance("label1")));
    assertTrue(labels.contains(NodeLabel.newInstance("label2")));
    verify(rmAdminClient).replaceLabelsOnNode(replaceLabelsArg.capture());
    Map<NodeId, Set<String>> nodeToLabels = replaceLabelsArg.getValue().getNodeToLabels();
    assertEquals(nodeToLabels.size(), 250);
    for (int i = 0; i < 100; i++) {
      assertTrue(nodeToLabels.get(NodeId.newInstance("host", i)).contains("label1"));
    }
    for (int i = 100; i < 250; i++) {
      assertTrue(nodeToLabels.get(NodeId.newInstance("host", i)).contains("label2"));
    }
  }
}
