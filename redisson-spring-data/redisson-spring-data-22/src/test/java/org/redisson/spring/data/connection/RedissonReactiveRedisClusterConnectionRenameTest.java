package org.redisson.spring.data.connection;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.redisson.ClusterRunner;
import org.redisson.ClusterRunner.ClusterProcesses;
import org.redisson.RedisRunner;
import org.redisson.RedisRunner.FailedToStartRedisException;
import org.redisson.Redisson;
import org.redisson.RedissonKeys;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SubscriptionMode;
import org.redisson.connection.balancer.RandomLoadBalancer;
import org.redisson.reactive.CommandReactiveService;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.redisson.connection.MasterSlaveConnectionManager.MAX_SLOT;

@RunWith(Parameterized.class)
public class RedissonReactiveRedisClusterConnectionRenameTest {

    @Parameterized.Parameters(name= "{index} - same slot = {0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {false},
                {true}
        });
    }

    @Parameterized.Parameter(0)
    public boolean sameSlot;

    static RedissonClient redisson;
    static RedissonReactiveRedisClusterConnection connection;
    static ClusterProcesses process;

    ByteBuffer originalKey = ByteBuffer.wrap("key".getBytes());
    ByteBuffer newKey = ByteBuffer.wrap("unset".getBytes());

    @BeforeClass
    public static void before() throws FailedToStartRedisException, IOException, InterruptedException {
        RedisRunner master1 = new RedisRunner().randomPort().randomDir().nosave();
        RedisRunner master2 = new RedisRunner().randomPort().randomDir().nosave();
        RedisRunner master3 = new RedisRunner().randomPort().randomDir().nosave();
        RedisRunner slave1 = new RedisRunner().randomPort().randomDir().nosave();
        RedisRunner slave2 = new RedisRunner().randomPort().randomDir().nosave();
        RedisRunner slave3 = new RedisRunner().randomPort().randomDir().nosave();


        ClusterRunner clusterRunner = new ClusterRunner()
                .addNode(master1, slave1)
                .addNode(master2, slave2)
                .addNode(master3, slave3);
        process = clusterRunner.run();

        Config config = new Config();
        config.useClusterServers()
                .setSubscriptionMode(SubscriptionMode.SLAVE)
                .setLoadBalancer(new RandomLoadBalancer())
                .addNodeAddress(process.getNodes().stream().findAny().get().getRedisServerAddressAndPort());

        redisson = Redisson.create(config);
        connection = new RedissonReactiveRedisClusterConnection(new CommandReactiveService(((RedissonKeys) redisson.getKeys()).getConnectionManager()));
    }

    @AfterClass
    public static void after() {
        process.shutdown();
        redisson.shutdown();
    }

    @After
    public void cleanup() {
        Mono<?> mono = connection.keyCommands().del(originalKey);
        Mono<?> mono1 = connection.keyCommands().del(newKey);

        mono.block();
        mono1.block();
    }

    @Test
    public void testRename() {
        ByteBuffer originalKey = ByteBuffer.wrap("key".getBytes());
        connection.stringCommands().set(originalKey, ByteBuffer.wrap("value".getBytes())).block();
        connection.keyCommands().expire(originalKey, Duration.ofSeconds(1000)).block();

        Integer originalSlot = connection.clusterGetSlotForKey(originalKey).block();
        ByteBuffer newKey = getNewKeyForSlot("key", getTargetSlot(originalSlot));

        connection.keyCommands().rename(originalKey, newKey).block();

        assertThat(connection.stringCommands().get(newKey).block()).isEqualTo("value".getBytes());
        assertThat(connection.keyCommands().ttl(newKey).block()).isGreaterThan(0);
    }

    protected ByteBuffer getNewKeyForSlot(String originalKey, Integer targetSlot) {
        int counter = 0;

        ByteBuffer newKey = ByteBuffer.wrap((originalKey + counter).getBytes());

        Integer newKeySlot = connection.clusterGetSlotForKey(newKey).block();

        while(!newKeySlot.equals(targetSlot)) {
            counter++;
            newKey = ByteBuffer.wrap((originalKey + counter).getBytes());
            newKeySlot = connection.clusterGetSlotForKey(newKey).block();
        }

        return newKey;
    }

    @Test
    public void testRenameNX() {
        connection.stringCommands().set(originalKey, ByteBuffer.wrap("value".getBytes())).block();
        connection.keyCommands().expire(originalKey, Duration.ofSeconds(1000)).block();

        Integer originalSlot = connection.clusterGetSlotForKey(originalKey).block();
        newKey = getNewKeyForSlot("key", getTargetSlot(originalSlot));

        Boolean result = connection.keyCommands().renameNX(originalKey, newKey).block();

        assertThat(connection.stringCommands().get(newKey).block()).isEqualTo("value".getBytes());
        assertThat(connection.keyCommands().ttl(newKey).block()).isGreaterThan(0);
        assertThat(result).isTrue();

        connection.stringCommands().set(originalKey, ByteBuffer.wrap("value".getBytes())).block();

        result = connection.keyCommands().renameNX(originalKey, newKey).block();

        assertThat(result).isFalse();
    }

    private Integer getTargetSlot(Integer originalSlot) {
        return sameSlot ? originalSlot : MAX_SLOT - originalSlot - 1;
    }

}
