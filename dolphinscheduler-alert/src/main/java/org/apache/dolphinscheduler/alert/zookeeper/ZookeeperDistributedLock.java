/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.alert.zookeeper;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.dolphinscheduler.alert.utils.Constants;
import org.apache.dolphinscheduler.alert.utils.PropertyUtils;
import org.apache.dolphinscheduler.common.utils.StringUtils;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Define the Zookeeper distributed lock
 */

public class ZookeeperDistributedLock extends AbstractLock {
    protected ZkClient zkClient;
    private CountDownLatch countDownLatch = null;
    private static String ZOOKEEPER_ROOT = "/zookeeperDistributedLock";

    public ZookeeperDistributedLock() {
        zkClient = new ZkClient(PropertyUtils.getString(Constants.ZOOKEEPER_QUORUM));
        if (!zkClient.exists(ZOOKEEPER_ROOT)) {
            zkClient.createPersistent(ZOOKEEPER_ROOT);
        }
    }

    @Override
    public boolean tryLock(String suffixPath) {
        boolean isLocked = false;
        if (!StringUtils.isNotEmpty(suffixPath)) {
            return isLocked;
        }
        if (!suffixPath.startsWith("/")) {
            suffixPath = "/" + suffixPath;
        }
        try {
            if (zkClient.exists(ZOOKEEPER_ROOT + suffixPath)) {
                isLocked = false;
            } else {
                zkClient.createEphemeral(ZOOKEEPER_ROOT + suffixPath);
                isLocked = true;
            }

        } catch (Exception e) {
            logger.error("zkClient.createEphemeral " + e.getMessage(), e);
            return isLocked;
        }
        logger.info("locked path '{}' {}", ZOOKEEPER_ROOT + suffixPath, isLocked);
        return isLocked;
    }

    public List<String> getNodes() {
        return zkClient.getChildren(ZOOKEEPER_ROOT);
    }

    /**
     * Listen to a node.
     */
    @Override
    public void waitLock(String suffixPath) {
        //Once zookeeper detects changes in node information,
        // it will trigger an anonymous anonymous callback function to notify the subscribed client, ie zkClient
        IZkDataListener iZkDataListener = new IZkDataListener() {

            public void handleDataDeleted(String path) throws Exception {
                // wake up the thread waiting
                if (countDownLatch != null) {
                    countDownLatch.countDown();
                }
            }

            public void handleDataChange(String path, Object data) throws Exception {
            }
        };
        // Register event listener
        zkClient.subscribeDataChanges(ZOOKEEPER_ROOT + suffixPath, iZkDataListener);

        // If the node exists, you need to wait until you receive the event notification
        if (zkClient.exists(suffixPath)) {
            countDownLatch = new CountDownLatch(1);
            try {
                countDownLatch.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        zkClient.unsubscribeDataChanges(ZOOKEEPER_ROOT + suffixPath, iZkDataListener);
    }

    /**
     * release the lock
     *
     * @param suffixPath zookeeper node path
     */
    @Override
    public boolean releaseLock(String suffixPath) {
        if (!StringUtils.isNotEmpty(suffixPath)) {
            return false;
        }
        if (!suffixPath.startsWith("/")) {
            suffixPath = "/" + suffixPath;
        }

        if (zkClient.exists(ZOOKEEPER_ROOT + suffixPath)) {
            logger.info("unlocked path '{}'", ZOOKEEPER_ROOT + suffixPath);
            return zkClient.delete(ZOOKEEPER_ROOT + suffixPath);
        }
        return true;
    }

    public void close() {
        if (zkClient != null) {
            zkClient.close();
        }
    }
}