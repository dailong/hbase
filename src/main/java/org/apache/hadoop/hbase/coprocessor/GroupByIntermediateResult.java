/*
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.coprocessor;

import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.expression.evaluation.StatsValue;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class GroupByIntermediateResult implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private List<EvaluationResult[]> topList;
  private Map<GroupByCombinedKey, List<StatsValue>> statsMap;

  public GroupByIntermediateResult() {
  }

  public GroupByIntermediateResult(List<EvaluationResult[]> topList,
      Map<GroupByCombinedKey, List<StatsValue>> statsMap) {
    this.topList = topList;
    this.statsMap = statsMap;
  }

  public List<EvaluationResult[]> getTopList() {
    return this.topList;
  }

  public void setTopList(List<EvaluationResult[]> topList) {
    this.topList = topList;
  }

  public Map<GroupByCombinedKey, List<StatsValue>> getStatsMap() {
    return this.statsMap;
  }

  public void setStatsMap(Map<GroupByCombinedKey, List<StatsValue>> statsMap) {
    this.statsMap = statsMap;
  }
}