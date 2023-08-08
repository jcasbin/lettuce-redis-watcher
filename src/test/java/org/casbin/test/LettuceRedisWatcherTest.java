package org.casbin.test;

import org.casbin.jcasbin.main.Enforcer;
import org.casbin.watcher.lettuce.LettuceRedisWatcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LettuceRedisWatcherTest {
    /**
     * LettuceRedisWatcher
     */
    private LettuceRedisWatcher lettuceRedisWatcher;

    /**
     * You should replace the initWatcher() method's content with your own Redis instance.
     */
    @Before
    public void initWatcher() {
        String redisTopic = "jcasbin-topic";
        this.lettuceRedisWatcher = new LettuceRedisWatcher("127.0.0.1", 6379, redisTopic, 2000, "foobared");
        Enforcer enforcer = new Enforcer();
        enforcer.setWatcher(this.lettuceRedisWatcher);
    }

    public void initClusterWatcher() {
        String redisTopic = "jcasbin-topic";
        // modify your cluster nodes. any one of these nodes.
        this.lettuceRedisWatcher = new LettuceRedisWatcher("192.168.1.234:6380,192.168.1.234:6381,192.168.1.234:6382", redisTopic, 2000, "123456");
        Enforcer enforcer = new Enforcer();
        enforcer.setWatcher(this.lettuceRedisWatcher);
    }

    @Test
    public void testUpdate() throws InterruptedException {
        // this.initClusterWatcher();
        this.lettuceRedisWatcher.update();
    }

    @Test
    public void testConsumerCallback() throws InterruptedException {
        // this.initClusterWatcher();
        // while (true) {
        this.lettuceRedisWatcher.setUpdateCallback((s) -> System.out.println(s));
        this.lettuceRedisWatcher.update();
        // }
    }

    @Test
    public void testConnectWatcherWithoutPassword() {
        String redisTopic = "jcasbin-topic";
        LettuceRedisWatcher lettuceRedisWatcherWithoutPassword = new LettuceRedisWatcher("127.0.0.1", 6378, redisTopic);
        Assert.assertNotNull(lettuceRedisWatcherWithoutPassword);
    }

    @Test
    public void testConnectWatcherCluster() {
        String redisTopic = "jcasbin-topic";
        LettuceRedisWatcher lettuceRedisWatcherCluster = new LettuceRedisWatcher("127.0.0.1:6380,127.0.0.1:6381,127.0.0.1:6382", redisTopic, 2000, null);
        Assert.assertNotNull(lettuceRedisWatcherCluster);
    }
}