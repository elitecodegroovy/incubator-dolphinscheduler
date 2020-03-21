package org.apache.dolphinscheduler.alert.zookeeper;

import org.junit.Assert;
import org.junit.Test;


public class ZookeeperDistributedLockTest {

    @Test
    public void tryLock() {
        int number = 10;
        AbstractLock distributedLock = new ZookeeperDistributedLock();

        Assert.assertFalse(distributedLock.getLockWithoutWaiting(""));
        Assert.assertFalse(distributedLock.getLockWithoutWaiting(null));

        for (int i = 0; i < number; i++) {
            String zkPath = "/" + i;
            Assert.assertTrue(distributedLock.getLockWithoutWaiting(zkPath));
        }
        // the path is locked.
        for (int i = 0; i < number; i++) {
            String zkPath = "/" + i;
            Assert.assertTrue(!distributedLock.getLockWithoutWaiting(zkPath));
        }
        for (int i = 0; i < number; i++) {
            String zkPath = "/" + i;
            distributedLock.releaseLock(zkPath);
        }
        distributedLock.close();
    }


    @Test
    public void releaseLock() {
        int number = 10;
        AbstractLock distributedLock = new ZookeeperDistributedLock();
        for (int i = 0; i < number; i++) {
            String zkPath = "/" + i;
            Assert.assertTrue(distributedLock.getLockWithoutWaiting(zkPath));
        }
        //release lock
        for (int i = 0; i < number; i++) {
            String zkPath = "/" + i;
            distributedLock.releaseLock(zkPath);
        }
        //get lock
        for (int i = 0; i < number; i++) {
            String zkPath = "/" + i;
            Assert.assertTrue(distributedLock.getLockWithoutWaiting(zkPath));
        }

    }

}