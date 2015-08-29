/**
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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;

/**
 * Context of the ResourceManager.
 * 资源管理器上下文接口类，定义了许多资源变量获取方法
 */
public interface RMContext {
  //获取中央分发调度器器实例
  Dispatcher getDispatcher();
  //资源应用状态存储对象
  RMStateStore getStateStore();
  //获取应用程序列表
  ConcurrentMap<ApplicationId, RMApp> getRMApps();
  //获取名称节点映射列表
  ConcurrentMap<String, RMNode> getInactiveRMNodes();
  //获取ID节点映射列表
  ConcurrentMap<NodeId, RMNode> getRMNodes();
  //获取运行中的AM监控线程
  AMLivelinessMonitor getAMLivelinessMonitor();
  //获取运行结束的AM监控线程
  AMLivelinessMonitor getAMFinishingMonitor();
  //获取超时监控对象
  ContainerAllocationExpirer getContainerAllocationExpirer();
  //认证相关
  DelegationTokenRenewer getDelegationTokenRenewer();
  AMRMTokenSecretManager getAMRMTokenSecretManager();
  RMContainerTokenSecretManager getContainerTokenSecretManager();
  NMTokenSecretManagerInRM getNMTokenSecretManager();
  ClientToAMTokenSecretManagerInRM getClientToAMTokenSecretManager();
  
  void setClientRMService(ClientRMService clientRMService);
  //获取用户服务对象
  ClientRMService getClientRMService();
  
  RMDelegationTokenSecretManager getRMDelegationTokenSecretManager();

  void setRMDelegationTokenSecretManager(
      RMDelegationTokenSecretManager delegationTokenSecretManager);
}