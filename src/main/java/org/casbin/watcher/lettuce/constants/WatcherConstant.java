package org.casbin.watcher.lettuce.constants;

/**
 * Created by IntelliJ IDEA 2023.
 * FileName: WatcherConstant.java
 *
 * @author shingmoyeung
 * @since 2023/8/5 21:02
 * @version 1.0
 * To change this template use File Or Preferences | Settings | Editor | File and Code Templates.
 * File Description: Redis Watcher Constant
 */
public class WatcherConstant {
    /**
     * Redis Type
     */
    public static final String LETTUCE_REDIS_TYPE_STANDALONE = "standalone";
    public static final String LETTUCE_REDIS_TYPE_CLUSTER = "cluster";

    /**
     * Redis URI
     */
    public static final String REDIS_URI_PREFIX = "redis://";
    public static final String REDIS_URI_PASSWORD_SPLIT = "@";
}