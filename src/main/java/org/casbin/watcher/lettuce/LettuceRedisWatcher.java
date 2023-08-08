package org.casbin.watcher.lettuce;

import io.lettuce.core.*;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.RedisClusterURIUtil;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import org.apache.commons.lang3.StringUtils;
import org.casbin.jcasbin.persist.Watcher;
import org.casbin.watcher.lettuce.constants.WatcherConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

public class LettuceRedisWatcher implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(LettuceRedisWatcher.class);
    private final String localId;
    private final String redisChannelName;
    private final AbstractRedisClient abstractRedisClient;
    private LettuceSubThread lettuceSubThread;
    private Runnable updateCallback;

    /**
     * Constructor
     *
     * @param redisIp          Redis IP
     * @param redisPort        Redis Port
     * @param redisChannelName Redis Channel
     * @param timeout          Redis Timeout
     * @param password         Redis Password
     */
    public LettuceRedisWatcher(String redisIp, Integer redisPort, String redisChannelName, int timeout, String password) {
        this.abstractRedisClient = this.getLettuceRedisClient(redisIp, redisPort, null, password, timeout, WatcherConstant.LETTUCE_REDIS_TYPE_STANDALONE);
        this.localId = UUID.randomUUID().toString();
        this.redisChannelName = redisChannelName;
        this.startSub();
    }

    /**
     * Constructor
     *
     * @param redisIp          Redis IP
     * @param redisPort        Redis Port
     * @param redisChannelName Redis Channel
     */
    public LettuceRedisWatcher(String redisIp, Integer redisPort, String redisChannelName) {
        this(redisIp, redisPort, redisChannelName, 2000, null);
    }

    /**
     * Constructor
     *
     * @param nodes            Redis Nodes
     * @param redisChannelName Redis Channel
     * @param timeout          Redis Timeout
     * @param password         Redis Password
     */
    public LettuceRedisWatcher(String nodes, String redisChannelName, Integer timeout, String password) {
        this.abstractRedisClient = this.getLettuceRedisClient(null, null, nodes, password, timeout, WatcherConstant.LETTUCE_REDIS_TYPE_CLUSTER);
        this.localId = UUID.randomUUID().toString();
        this.redisChannelName = redisChannelName;
        this.startSub();
    }

    @Override
    public void setUpdateCallback(Runnable runnable) {
        this.updateCallback = runnable;
        lettuceSubThread.setUpdateCallback(runnable);
    }

    @Override
    public void setUpdateCallback(Consumer<String> consumer) {
        this.lettuceSubThread.setUpdateCallback(consumer);
    }

    @Override
    public void update() {
        try (StatefulRedisPubSubConnection<String, String> statefulRedisPubSubConnection =
                     this.getStatefulRedisPubSubConnection(this.abstractRedisClient)) {
            if (statefulRedisPubSubConnection.isOpen()) {
                String msg = "Casbin policy has a new version from redis watcher: ".concat(this.localId);
                statefulRedisPubSubConnection.async().publish(this.redisChannelName, msg);
            }
        }
    }

    private void startSub() {
        this.lettuceSubThread = new LettuceSubThread(this.abstractRedisClient, this.redisChannelName, this.updateCallback);
        this.lettuceSubThread.start();
    }

    /**
     * Initialize the Redis Client
     *
     * @param host     Redis Host
     * @param port     Redis Port
     * @param nodes    Redis Nodes
     * @param password Redis Password
     * @param timeout  Redis Timeout
     * @param type     Redis Type (standalone | cluster) default:standalone
     * @return AbstractRedisClient
     */
    private AbstractRedisClient getLettuceRedisClient(String host, Integer port, String nodes, String password, int timeout, String type) {
        // todo default standalone ?
        // type = StringUtils.isEmpty(type) ? WatcherConstant.LETTUCE_REDIS_TYPE_STANDALONE : type;
        if (StringUtils.isNotEmpty(type) && StringUtils.equalsAnyIgnoreCase(type,
                WatcherConstant.LETTUCE_REDIS_TYPE_STANDALONE, WatcherConstant.LETTUCE_REDIS_TYPE_CLUSTER)) {
            ClientResources clientResources = DefaultClientResources.builder()
                    .ioThreadPoolSize(4)
                    .computationThreadPoolSize(4)
                    .build();
            if (StringUtils.equalsIgnoreCase(type, WatcherConstant.LETTUCE_REDIS_TYPE_STANDALONE)) {
                // standalone
                RedisURI redisUri = null;
                if (StringUtils.isNotEmpty(password)) {
                    redisUri = RedisURI.builder()
                            .withHost(host)
                            .withPort(port)
                            .withPassword(password.toCharArray())
                            .withTimeout(Duration.of(timeout, ChronoUnit.SECONDS))
                            .build();
                } else {
                    redisUri = RedisURI.builder()
                            .withHost(host)
                            .withPort(port)
                            .withTimeout(Duration.of(timeout, ChronoUnit.SECONDS))
                            .build();
                }
                ClientOptions clientOptions = ClientOptions.builder()
                        .autoReconnect(true)
                        .pingBeforeActivateConnection(true)
                        .build();
                RedisClient redisClient = RedisClient.create(clientResources, redisUri);
                redisClient.setOptions(clientOptions);
                return redisClient;
            } else {
                // cluster
                TimeoutOptions timeoutOptions = TimeoutOptions.builder().fixedTimeout(Duration.of(timeout, ChronoUnit.SECONDS)).build();
                ClusterTopologyRefreshOptions topologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
                        .enablePeriodicRefresh(Duration.of(10, ChronoUnit.MINUTES))
                        .enableAdaptiveRefreshTrigger(ClusterTopologyRefreshOptions.RefreshTrigger.MOVED_REDIRECT, ClusterTopologyRefreshOptions.RefreshTrigger.PERSISTENT_RECONNECTS)
                        .adaptiveRefreshTriggersTimeout(Duration.of(30, ChronoUnit.SECONDS))
                        .build();
                ClusterClientOptions clusterClientOptions = ClusterClientOptions.builder()
                        .autoReconnect(true)
                        .timeoutOptions(timeoutOptions)
                        .topologyRefreshOptions(topologyRefreshOptions)
                        .pingBeforeActivateConnection(true)
                        .validateClusterNodeMembership(true)
                        .build();
                // Redis Cluster Node
                String redisUri = StringUtils.isNotEmpty(password) ?
                        WatcherConstant.REDIS_URI_PREFIX.concat(password).concat(WatcherConstant.REDIS_URI_PASSWORD_SPLIT).concat(nodes) :
                        WatcherConstant.REDIS_URI_PREFIX.concat(nodes);
                logger.info("Redis Cluster Uri: {}", redisUri);
                List<RedisURI> redisURIList = RedisClusterURIUtil.toRedisURIs(URI.create(redisUri));
                RedisClusterClient redisClusterClient = RedisClusterClient.create(clientResources, redisURIList);
                redisClusterClient.setOptions(clusterClientOptions);
                return redisClusterClient;
            }
        } else {
            throw new IllegalArgumentException("Redis-Type is required and can only be [standalone] or [cluster]");
        }
    }

    /**
     * Get Redis PubSub Connection
     *
     * @param abstractRedisClient Redis Client
     * @return StatefulRedisPubSubConnection
     */
    private StatefulRedisPubSubConnection<String, String> getStatefulRedisPubSubConnection(AbstractRedisClient abstractRedisClient) {
        if (abstractRedisClient instanceof RedisClient) {
            return ((RedisClient) abstractRedisClient).connectPubSub();
        } else {
            return ((RedisClusterClient) abstractRedisClient).connectPubSub();
        }
    }
}