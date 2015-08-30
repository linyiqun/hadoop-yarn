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

package org.apache.hadoop.yarn.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.service.AbstractService;

/**
 * A simple liveliness monitor with which clients can register, trust the
 * component to monitor liveliness, get a call-back on expiry and then finally
 * unregister.
 */
@Public
@Evolving
//进程存活状态监控类
public abstract class AbstractLivelinessMonitor<O> extends AbstractService {

  private static final Log LOG = LogFactory.getLog(AbstractLivelinessMonitor.class);

  //thread which runs periodically to see the last time since a heartbeat is
  //received.
  //检查线程
  private Thread checkerThread;
  private volatile boolean stopped;
  //默认超时时间5分钟
  public static final int DEFAULT_EXPIRE = 5*60*1000;//5 mins
  //超时时间
  private int expireInterval = DEFAULT_EXPIRE;
  //监控间隔检测时间，为超时时间的1/3
  private int monitorInterval = expireInterval/3;

  private final Clock clock;
  
  //保存了心跳检验的结果记录
  private Map<O, Long> running = new HashMap<O, Long>();

  public AbstractLivelinessMonitor(String name, Clock clock) {
    super(name);
    this.clock = clock;
  }

  @Override
  protected void serviceStart() throws Exception {
    assert !stopped : "starting when already stopped";
    checkerThread = new Thread(new PingChecker());
    checkerThread.setName("Ping Checker");
    checkerThread.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopped = true;
    if (checkerThread != null) {
      checkerThread.interrupt();
    }
    super.serviceStop();
  }

  protected abstract void expire(O ob);

  protected void setExpireInterval(int expireInterval) {
    this.expireInterval = expireInterval;
  }

  protected void setMonitorInterval(int monitorInterval) {
    this.monitorInterval = monitorInterval;
  }
  
  //更新心跳监控检测最新时间
  public synchronized void receivedPing(O ob) {
    //only put for the registered objects
    if (running.containsKey(ob)) {
      running.put(ob, clock.getTime());
    }
  }

  //新的节点注册心跳监控
  public synchronized void register(O ob) {
    running.put(ob, clock.getTime());
  }
  
  //节点移除心跳监控
  public synchronized void unregister(O ob) {
    running.remove(ob);
  }

  private class PingChecker implements Runnable {

    @Override
    public void run() {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        synchronized (AbstractLivelinessMonitor.this) {
          Iterator<Map.Entry<O, Long>> iterator = 
            running.entrySet().iterator();

          //avoid calculating current time everytime in loop
          long currentTime = clock.getTime();

          while (iterator.hasNext()) {
            Map.Entry<O, Long> entry = iterator.next();
            //进行超时检测
            if (currentTime > entry.getValue() + expireInterval) {
              iterator.remove();
              //调用超时处理方法，将处理事件交由调度器处理
              expire(entry.getKey());
              LOG.info("Expired:" + entry.getKey().toString() + 
                      " Timed out after " + expireInterval/1000 + " secs");
            }
          }
        }
        try {
          Thread.sleep(monitorInterval);
        } catch (InterruptedException e) {
          LOG.info(getName() + " thread interrupted");
          break;
        }
      }
    }
  }

}
