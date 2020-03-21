package org.apache.dolphinscheduler.alert.zookeeper;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AbstractLock implements Lock {
    protected static final Logger logger = LoggerFactory.getLogger(AbstractLock.class);

    public boolean getLockWithWaiting(String suffixPath) {
        if (tryLock(suffixPath)) {
            logger.info("get the lock path {} ", suffixPath);
        } else {
            waitLock(suffixPath);
            return getLockWithWaiting(suffixPath);
        }
        return true;
    }

    public boolean getLockWithoutWaiting(String suffixPath) {
        if (tryLock(suffixPath)) {
            return true;
        }
        return false;
    }

    public List<String> getRootNodes() {
        return getNodes();
    }

    public abstract boolean tryLock(String suffixPath);

    public abstract void waitLock(String suffixPath);

    public abstract List<String> getNodes();

    public abstract void close();
}