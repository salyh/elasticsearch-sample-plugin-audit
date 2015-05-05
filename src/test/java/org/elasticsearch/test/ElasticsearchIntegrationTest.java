/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.InternalTestCluster.clusterName;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.InetSocketAddress;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.http.impl.client.HttpClients;
import org.apache.lucene.store.StoreRateLimiting;
import org.apache.lucene.util.AbstractRandomizedTest;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.base.Joiner;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMapper.Loading;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.mapper.internal.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;
import org.elasticsearch.index.mapper.internal.SizeFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.merge.policy.AbstractMergePolicyProvider;
import org.elasticsearch.index.merge.policy.LogByteSizeMergePolicyProvider;
import org.elasticsearch.index.merge.policy.LogDocMergePolicyProvider;
import org.elasticsearch.index.merge.policy.MergePolicyModule;
import org.elasticsearch.index.merge.policy.MergePolicyProvider;
import org.elasticsearch.index.merge.policy.TieredMergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.ConcurrentMergeSchedulerProvider;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerModule;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.merge.scheduler.SerialMergeSchedulerProvider;
import org.elasticsearch.index.store.StoreModule;
import org.elasticsearch.index.translog.TranslogService;
import org.elasticsearch.index.translog.fs.FsTranslog;
import org.elasticsearch.index.translog.fs.FsTranslogFile;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.cache.filter.IndicesFilterCache;
import org.elasticsearch.indices.cache.query.IndicesQueryCache;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.client.RandomizingClient;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.Randomness;
import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;

/**
 * {@link ElasticsearchIntegrationTest} is an abstract base class to run integration
 * tests against a JVM private Elasticsearch Cluster. The test class supports 2 different
 * cluster scopes.
 * <ul>
 * <li>{@link Scope#TEST} - uses a new cluster for each individual test method.</li>
 * <li>{@link Scope#SUITE} - uses a cluster shared across all test methods in the same suite</li>
 * </ul>
 * <p/>
 * The most common test scope is {@link Scope#SUITE} which shares a cluster per test suite.
 * <p/>
 * If the test methods need specific node settings or change persistent and/or transient cluster settings {@link Scope#TEST} should be used.
 * To configure a scope for the test cluster the {@link ClusterScope} annotation should be used, here is an example:
 * 
 * <pre>
 *
 * &#064;ClusterScope(scope = Scope.TEST)
 * public class SomeIntegrationTest extends ElasticsearchIntegrationTest {
 *     &#064;Test
 *     public void testMethod() {
 *     }
 * }
 * </pre>
 * <p/>
 * If no {@link ClusterScope} annotation is present on an integration test the default scope is {@link Scope#SUITE}
 * <p/>
 * A test cluster creates a set of nodes in the background before the test starts. The number of nodes in the cluster is determined at
 * random and can change across tests. The {@link ClusterScope} allows configuring the initial number of nodes that are created before the
 * tests start.
 * <p/>
 * 
 * <pre>
 * &#064;ClusterScope(scope = Scope.SUITE, numDataNodes = 3)
 * public class SomeIntegrationTest extends ElasticsearchIntegrationTest {
 *     &#064;Test
 *     public void testMethod() {
 *     }
 * }
 * </pre>
 * <p/>
 * Note, the {@link ElasticsearchIntegrationTest} uses randomized settings on a cluster and index level. For instance each test might use
 * different directory implementation for each test or will return a random client to one of the nodes in the cluster for each call to
 * {@link #client()}. Test failures might only be reproducible if the correct system properties are passed to the test execution
 * environment.
 * <p/>
 * <p>
 * This class supports the following system properties (passed with -Dkey=value to the application)
 * <ul>
 * <li>-D{@value #TESTS_CLIENT_RATIO} - a double value in the interval [0..1] which defines the ration between node and transport clients
 * used</li>
 * <li>-D{@value InternalTestCluster#TESTS_ENABLE_MOCK_MODULES} - a boolean value to enable or disable mock modules. This is useful to test
 * the system without asserting modules that to make sure they don't hide any bugs in production.</li>
 * <li>- a random seed used to initialize the index random context.
 * </ul>
 * </p>
 */
@Ignore
@AbstractRandomizedTest.Integration
public abstract class ElasticsearchIntegrationTest extends ElasticsearchTestCase {

    /** node names of the corresponding clusters will start with these prefixes */
    public static final String SUITE_CLUSTER_NODE_PREFIX = "node_s";
    public static final String TEST_CLUSTER_NODE_PREFIX = "node_t";

    /**
     * Key used to set the transport client ratio via the commandline -D{@value #TESTS_CLIENT_RATIO}
     */
    public static final String TESTS_CLIENT_RATIO = "tests.client.ratio";

    /**
     * Key used to eventually switch to using an external cluster and provide its transport addresses
     */
    public static final String TESTS_CLUSTER = "tests.cluster";

    /**
     * Key used to retrieve the index random seed from the index settings on a running node.
     * The value of this seed can be used to initialize a random context for a specific index.
     * It's set once per test via a generic index template.
     */
    public static final String SETTING_INDEX_SEED = "index.tests.seed";

    /**
     * Threshold at which indexing switches from frequently async to frequently bulk.
     */
    private static final int FREQUENT_BULK_THRESHOLD = 300;

    /**
     * Threshold at which bulk indexing will always be used.
     */
    private static final int ALWAYS_BULK_THRESHOLD = 3000;

    /**
     * Maximum number of async operations that indexRandom will kick off at one time.
     */
    private static final int MAX_IN_FLIGHT_ASYNC_INDEXES = 150;

    /**
     * Maximum number of documents in a single bulk index request.
     */
    private static final int MAX_BULK_INDEX_REQUEST_SIZE = 1000;

    /**
     * Default minimum number of shards for an index
     */
    protected static final int DEFAULT_MIN_NUM_SHARDS = 1;

    /**
     * Default maximum number of shards for an index
     */
    protected static final int DEFAULT_MAX_NUM_SHARDS = 10;

    /**
     * The current cluster depending on the configured {@link Scope}.
     * By default if no {@link ClusterScope} is configured this will hold a reference to the suite cluster.
     */
    private static TestCluster currentCluster;

    private static final double TRANSPORT_CLIENT_RATIO = transportClientRatio();

    private static final Map<Class<?>, TestCluster> clusters = new IdentityHashMap<>();

    private static ElasticsearchIntegrationTest INSTANCE = null; // see @SuiteScope
    private static Long SUITE_SEED = null;

    @BeforeClass
    public static void beforeClass() throws Exception {
        SUITE_SEED = randomLong();
        initializeSuiteScope();
    }

    protected final void beforeInternal() throws Exception {
        assert Thread.getDefaultUncaughtExceptionHandler() instanceof ElasticsearchUncaughtExceptionHandler;
        try {
            final Scope currentClusterScope = getCurrentClusterScope();
            switch (currentClusterScope) {
                case SUITE:
                    assert SUITE_SEED != null : "Suite seed was not initialized";
                    currentCluster = buildAndPutCluster(currentClusterScope, SUITE_SEED);
                    break;
                case TEST:
                    currentCluster = buildAndPutCluster(currentClusterScope, randomLong());
                    break;
                default:
                    fail("Unknown Scope: [" + currentClusterScope + "]");
            }
            cluster().beforeTest(getRandom(), getPerTestTransportClientRatio());
            //cluster().wipe();
            randomIndexTemplate();
            printTestMessage("before");
        } catch (final OutOfMemoryError e) {
            if (e.getMessage().contains("unable to create new native thread")) {
                ElasticsearchTestCase.printStackDump(logger);
            }
            throw e;
        }
    }

    private void printTestMessage(final String message) {
        if (isSuiteScopedTest(getClass())) {
            logger.info("[{}]: {} suite", getTestClass().getSimpleName(), message);
        } else {
            logger.info("[{}#{}]: {} test", getTestClass().getSimpleName(), getTestName(), message);
        }
    }

    private Loading randomLoadingValues() {
        if (compatibilityVersion().onOrAfter(Version.V_1_2_0)) {
            // Loading.EAGER_GLOBAL_ORDINALS was added in 1,2.0
            return randomFrom(Loading.values());
        } else {
            return randomFrom(Loading.LAZY, Loading.EAGER);
        }

    }

    /**
     * Creates a randomized index template. This template is used to pass in randomized settings on a
     * per index basis. Allows to enable/disable the randomization for number of shards and replicas
     */
    private void randomIndexTemplate() throws IOException {

        // TODO move settings for random directory etc here into the index based randomized settings.
        if (cluster().size() > 0) {
            final ImmutableSettings.Builder randomSettingsBuilder = setRandomSettings(getRandom(), ImmutableSettings.builder()).put(
                    SETTING_INDEX_SEED, getRandom().nextLong());

            if (randomizeNumberOfShardsAndReplicas()) {
                randomSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, numberOfShards()).put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas());
            }
            XContentBuilder mappings = null;
            if (frequently() && randomDynamicTemplates()) {
                mappings = XContentFactory.jsonBuilder().startObject().startObject("_default_");
                if (randomBoolean()) {
                    mappings.startObject(IdFieldMapper.NAME).field("index", randomFrom("not_analyzed", "no")).endObject();
                }
                if (randomBoolean()) {
                    mappings.startObject(TypeFieldMapper.NAME).field("index", randomFrom("no", "not_analyzed")).endObject();
                }
                if (randomBoolean()) {
                    mappings.startObject(TimestampFieldMapper.NAME).field("enabled", randomBoolean()).startObject("fielddata")
                            .field(FieldDataType.FORMAT_KEY, randomFrom("array", "doc_values")).endObject().endObject();
                }
                if (randomBoolean()) {
                    mappings.startObject(SizeFieldMapper.NAME).field("enabled", randomBoolean()).endObject();
                }
                if (randomBoolean()) {
                    mappings.startObject(AllFieldMapper.NAME).field("auto_boost", true).endObject();
                }
                if (randomBoolean()) {
                    mappings.startObject(SourceFieldMapper.NAME).field("compress", randomBoolean()).endObject();
                }
                if (compatibilityVersion().onOrAfter(Version.V_1_3_0)) {
                    mappings.startObject(FieldNamesFieldMapper.NAME).startObject("fielddata")
                            .field(FieldDataType.FORMAT_KEY, randomFrom("paged_bytes", "fst", "doc_values")).endObject().endObject();
                }
                mappings.startArray("dynamic_templates")
                        .startObject()
                        .startObject("template-strings")
                        .field("match_mapping_type", "string")
                        .startObject("mapping")
                        .startObject("fielddata")
                        .field(FieldDataType.FORMAT_KEY, randomFrom("paged_bytes", "fst"))
                // unfortunately doc values only work on not_analyzed fields
                        .field(Loading.KEY, randomLoadingValues()).endObject().endObject().endObject().endObject().startObject()
                        .startObject("template-longs").field("match_mapping_type", "long").startObject("mapping").startObject("fielddata")
                        .field(FieldDataType.FORMAT_KEY, randomFrom("array", "doc_values"))
                        .field(Loading.KEY, randomFrom(Loading.LAZY, Loading.EAGER)).endObject().endObject().endObject().endObject()
                        .startObject().startObject("template-doubles").field("match_mapping_type", "double").startObject("mapping")
                        .startObject("fielddata").field(FieldDataType.FORMAT_KEY, randomFrom("array", "doc_values"))
                        .field(Loading.KEY, randomFrom(Loading.LAZY, Loading.EAGER)).endObject().endObject().endObject().endObject()
                        .startObject().startObject("template-geo_points").field("match_mapping_type", "geo_point").startObject("mapping")
                        .startObject("fielddata").field(FieldDataType.FORMAT_KEY, randomFrom("array", "doc_values"))
                        .field(Loading.KEY, randomFrom(Loading.LAZY, Loading.EAGER)).endObject().endObject().endObject().endObject()
                        .endArray();
                mappings.endObject().endObject();
            }

            final PutIndexTemplateRequestBuilder putTemplate = client().admin().indices().preparePutTemplate("random_index_template")
                    .setTemplate("*").setOrder(0).setSettings(randomSettingsBuilder);
            if (mappings != null) {
                logger.info("test using _default_ mappings: [{}]", mappings.bytesStream().bytes().toUtf8());
                putTemplate.addMapping("_default_", mappings);
            }
            assertAcked(putTemplate.execute().actionGet());
        }
    }

    protected boolean randomizeNumberOfShardsAndReplicas() {
        return compatibilityVersion().onOrAfter(Version.V_1_1_0);
    }

    private static ImmutableSettings.Builder setRandomSettings(final Random random, final ImmutableSettings.Builder builder) {
        setRandomMerge(random, builder);
        setRandomTranslogSettings(random, builder);
        setRandomNormsLoading(random, builder);
        setRandomScriptingSettings(random, builder);
        if (random.nextBoolean()) {
            if (random.nextInt(10) == 0) { // do something crazy slow here
                builder.put(IndicesStore.INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC,
                        new ByteSizeValue(RandomInts.randomIntBetween(random, 1, 10), ByteSizeUnit.MB));
            } else {
                builder.put(IndicesStore.INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC,
                        new ByteSizeValue(RandomInts.randomIntBetween(random, 10, 200), ByteSizeUnit.MB));
            }
        }
        if (random.nextBoolean()) {
            builder.put(IndicesStore.INDICES_STORE_THROTTLE_TYPE, RandomPicks.randomFrom(random, StoreRateLimiting.Type.values()));
        }

        if (random.nextBoolean()) {
            builder.put(StoreModule.DISTIBUTOR_KEY, random.nextBoolean() ? StoreModule.LEAST_USED_DISTRIBUTOR
                    : StoreModule.RANDOM_WEIGHT_DISTRIBUTOR);
        }

        if (random.nextBoolean()) {
            if (random.nextInt(10) == 0) { // do something crazy slow here
                builder.put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC,
                        new ByteSizeValue(RandomInts.randomIntBetween(random, 1, 10), ByteSizeUnit.MB));
            } else {
                builder.put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC,
                        new ByteSizeValue(RandomInts.randomIntBetween(random, 10, 200), ByteSizeUnit.MB));
            }
        }

        if (random.nextBoolean()) {
            builder.put(RecoverySettings.INDICES_RECOVERY_COMPRESS, random.nextBoolean());
        }

        if (random.nextBoolean()) {
            builder.put(FsTranslog.INDEX_TRANSLOG_FS_TYPE, RandomPicks.randomFrom(random, FsTranslogFile.Type.values()).name());
        }

        if (random.nextBoolean()) {
            builder.put(IndicesQueryCache.INDEX_CACHE_QUERY_ENABLED, random.nextBoolean());
        }

        if (random.nextBoolean()) {
            builder.put(IndicesQueryCache.INDICES_CACHE_QUERY_CONCURRENCY_LEVEL, RandomInts.randomIntBetween(random, 1, 32));
            builder.put(IndicesFieldDataCache.FIELDDATA_CACHE_CONCURRENCY_LEVEL, RandomInts.randomIntBetween(random, 1, 32));
            builder.put(IndicesFilterCache.INDICES_CACHE_FILTER_CONCURRENCY_LEVEL, RandomInts.randomIntBetween(random, 1, 32));
        }
        if (globalCompatibilityVersion().before(Version.V_1_3_2)) {
            // if we test against nodes before 1.3.2 we disable all the compression due to a known bug
            // see #7210
            builder.put(RecoverySettings.INDICES_RECOVERY_COMPRESS, false);
        }
        return builder;
    }

    private static ImmutableSettings.Builder setRandomScriptingSettings(final Random random, final ImmutableSettings.Builder builder) {
        if (random.nextBoolean()) {
            builder.put(ScriptService.SCRIPT_CACHE_SIZE_SETTING, RandomInts.randomIntBetween(random, -100, 2000));
        }
        if (random.nextBoolean()) {
            builder.put(ScriptService.SCRIPT_CACHE_EXPIRE_SETTING,
                    TimeValue.timeValueMillis(RandomInts.randomIntBetween(random, 750, 10000000)));
        }
        return builder;
    }

    private static ImmutableSettings.Builder setRandomMerge(final Random random, final ImmutableSettings.Builder builder) {
        if (random.nextBoolean()) {
            builder.put(AbstractMergePolicyProvider.INDEX_COMPOUND_FORMAT,
                    random.nextBoolean() ? random.nextDouble() : random.nextBoolean());
        }
        Class<? extends MergePolicyProvider<?>> mergePolicy = TieredMergePolicyProvider.class;
        switch (random.nextInt(5)) {
            case 4:
                mergePolicy = LogByteSizeMergePolicyProvider.class;
                break;
            case 3:
                mergePolicy = LogDocMergePolicyProvider.class;
                break;
            case 0:
                mergePolicy = null;
        }
        if (mergePolicy != null) {
            builder.put(MergePolicyModule.MERGE_POLICY_TYPE_KEY, mergePolicy.getName());
        }

        if (random.nextBoolean()) {
            builder.put(MergeSchedulerProvider.FORCE_ASYNC_MERGE, random.nextBoolean());
        }
        switch (random.nextInt(5)) {
            case 4:
                builder.put(MergeSchedulerModule.MERGE_SCHEDULER_TYPE_KEY, SerialMergeSchedulerProvider.class.getName());
                break;
            case 3:
                builder.put(MergeSchedulerModule.MERGE_SCHEDULER_TYPE_KEY, ConcurrentMergeSchedulerProvider.class);
                final int maxThreadCount = RandomInts.randomIntBetween(random, 1, 4);
                final int maxMergeCount = RandomInts.randomIntBetween(random, maxThreadCount, maxThreadCount + 4);
                builder.put(ConcurrentMergeSchedulerProvider.MAX_MERGE_COUNT, maxMergeCount);
                builder.put(ConcurrentMergeSchedulerProvider.MAX_THREAD_COUNT, maxThreadCount);
                break;
        }

        return builder;
    }

    private static ImmutableSettings.Builder setRandomNormsLoading(final Random random, final ImmutableSettings.Builder builder) {
        if (random.nextBoolean()) {
            builder.put(SearchService.NORMS_LOADING_KEY,
                    RandomPicks.randomFrom(random, Arrays.asList(FieldMapper.Loading.EAGER, FieldMapper.Loading.LAZY)));
        }
        return builder;
    }

    private static ImmutableSettings.Builder setRandomTranslogSettings(final Random random, final ImmutableSettings.Builder builder) {
        if (random.nextBoolean()) {
            builder.put(TranslogService.INDEX_TRANSLOG_FLUSH_THRESHOLD_OPS, RandomInts.randomIntBetween(random, 1, 10000));
        }
        if (random.nextBoolean()) {
            builder.put(TranslogService.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE, new ByteSizeValue(RandomInts.randomIntBetween(random, 1, 300),
                    ByteSizeUnit.MB));
        }
        if (random.nextBoolean()) {
            builder.put(TranslogService.INDEX_TRANSLOG_FLUSH_THRESHOLD_PERIOD,
                    TimeValue.timeValueMinutes(RandomInts.randomIntBetween(random, 1, 60)));
        }
        if (random.nextBoolean()) {
            builder.put(TranslogService.INDEX_TRANSLOG_FLUSH_INTERVAL,
                    TimeValue.timeValueMillis(RandomInts.randomIntBetween(random, 1, 10000)));
        }
        if (random.nextBoolean()) {
            builder.put(TranslogService.INDEX_TRANSLOG_DISABLE_FLUSH, random.nextBoolean());
        }
        return builder;
    }

    private TestCluster buildWithPrivateContext(final Scope scope, final long seed) throws Exception {
        return RandomizedContext.current().runWithPrivateRandomness(new Randomness(seed), new Callable<TestCluster>() {
            @Override
            public TestCluster call() throws Exception {
                return buildTestCluster(scope, seed);
            }
        });
    }

    private TestCluster buildAndPutCluster(final Scope currentClusterScope, final long seed) throws Exception {
        final Class<?> clazz = this.getClass();
        TestCluster testCluster = clusters.remove(clazz); // remove this cluster first
        clearClusters(); // all leftovers are gone by now... this is really just a double safety if we miss something somewhere
        switch (currentClusterScope) {
            case SUITE:
                if (testCluster == null) { // only build if it's not there yet
                    testCluster = buildWithPrivateContext(currentClusterScope, seed);
                }
                break;
            case TEST:
                // close the previous one and create a new one
                IOUtils.closeWhileHandlingException(testCluster);
                testCluster = buildTestCluster(currentClusterScope, seed);
                break;
        }
        clusters.put(clazz, testCluster);
        return testCluster;
    }

    private static void clearClusters() throws IOException {
        if (!clusters.isEmpty()) {
            IOUtils.close(clusters.values());
            clusters.clear();
        }
    }

    protected final void afterInternal(final boolean afterClass) throws Exception {
        boolean success = false;
        try {
            final Scope currentClusterScope = getCurrentClusterScope();
            printTestMessage("cleaning up after");
            clearDisruptionScheme();
            try {
                if (cluster() != null) {
                    if (currentClusterScope != Scope.TEST) {
                        final MetaData metaData = client().admin().cluster().prepareState().execute().actionGet().getState().getMetaData();
                        assertThat("test leaves persistent cluster metadata behind: " + metaData.persistentSettings().getAsMap(), metaData
                                .persistentSettings().getAsMap().size(), equalTo(0));
                        assertThat("test leaves transient cluster metadata behind: " + metaData.transientSettings().getAsMap(), metaData
                                .transientSettings().getAsMap().size(), equalTo(0));
                    }
                    ensureClusterSizeConsistency();
                    cluster().wipe(); // wipe after to make sure we fail in the test that didn't ack the delete
                    if (afterClass || currentClusterScope == Scope.TEST) {
                        cluster().close();
                    }
                    cluster().assertAfterTest();
                }
            } finally {
                if (currentClusterScope == Scope.TEST) {
                    clearClusters(); // it is ok to leave persistent / transient cluster state behind if scope is TEST
                }
            }
            printTestMessage("cleaned up after");
            success = true;
        } finally {
            if (!success) {
                // if we failed here that means that something broke horribly so we should clear all clusters
                afterTestRule.forceFailure();
            }
        }
    }

    @Override
    protected final AfterTestRule.Task afterTestTask() {
        return new AfterTestRule.Task() {
            @Override
            public void onTestFailed() {
                //we can't clear clusters after failure when using suite scoped tests, as we would need to call again
                //initializeSuiteScope but that is static and can only be called from beforeClass
                if (runTestScopeLifecycle()) {
                    // If there was a problem during the afterTest, we clear all clusters.
                    // We do the same in case we just had a test failure to make sure subsequent
                    // tests get a new / clean cluster
                    try {
                        clearClusters();
                    } catch (final IOException e) {
                        throw new RuntimeException("unable to clear clusters", e);
                    }
                    afterTestFailed();
                    currentCluster = null;
                }
            }

            @Override
            public void onTestFinished() {
                if (runTestScopeLifecycle()) {
                    if (currentCluster != null) {
                        // this can be null if the test fails due to static initialization ie. missing parameter on the cmd
                        try {
                            currentCluster.afterTest();
                        } catch (final IOException e) {
                            throw new RuntimeException("error during afterTest", e);
                        }
                        currentCluster = null;
                    }
                }
            }
        };
    }

    /**
     * Allows to execute some additional task after a test is failed, right after we cleared the clusters
     */
    protected void afterTestFailed() {

    }

    public static TestCluster cluster() {
        return currentCluster;
    }

    public static boolean isInternalCluster() {
        return (currentCluster instanceof InternalTestCluster);
    }

    public static InternalTestCluster internalCluster() {
        if (!isInternalCluster()) {
            throw new UnsupportedOperationException("current test cluster is immutable");
        }
        return (InternalTestCluster) currentCluster;
    }

    public ClusterService clusterService() {
        return internalCluster().clusterService();
    }

    public static Client client() {
        return client(null);
    }

    public static Client client(@Nullable final String node) {
        if (node != null) {
            return internalCluster().client(node);
        }
        Client client = cluster().client();
        if (frequently()) {
            client = new RandomizingClient(client, getRandom());
        }
        return client;
    }

    public static Client dataNodeClient() {
        Client client = internalCluster().dataNodeClient();
        if (frequently()) {
            client = new RandomizingClient(client, getRandom());
        }
        return client;
    }

    public static Iterable<Client> clients() {
        return cluster();
    }

    protected int minimumNumberOfShards() {
        return DEFAULT_MIN_NUM_SHARDS;
    }

    protected int maximumNumberOfShards() {
        return DEFAULT_MAX_NUM_SHARDS;
    }

    protected int numberOfShards() {
        return between(minimumNumberOfShards(), maximumNumberOfShards());
    }

    protected int minimumNumberOfReplicas() {
        return 0;
    }

    protected int maximumNumberOfReplicas() {
        //use either 0 or 1 replica, yet a higher amount when possible, but only rarely
        final int maxNumReplicas = Math.max(0, cluster().numDataNodes() - 1);
        return frequently() ? Math.min(1, maxNumReplicas) : maxNumReplicas;
    }

    protected int numberOfReplicas() {
        return between(minimumNumberOfReplicas(), maximumNumberOfReplicas());
    }

    public void setDisruptionScheme(final ServiceDisruptionScheme scheme) {
        internalCluster().setDisruptionScheme(scheme);
    }

    public void clearDisruptionScheme() {
        if (isInternalCluster()) {
            internalCluster().clearDisruptionScheme();
        }
    }

    /** overridable method to turn custom data paths on or off */
    public boolean useCustomDataPath() {
        return true;
    }

    /**
     * Returns a settings object used in {@link #createIndex(String...)} and {@link #prepareCreate(String)} and friends.
     * This method can be overwritten by subclasses to set defaults for the indices that are created by the test.
     * By default it returns a settings object that sets a random number of shards. Number of shards and replicas
     * can be controlled through specific methods.
     */
    public Settings indexSettings() {
        final ImmutableSettings.Builder builder = ImmutableSettings.builder();
        if (randomizeNumberOfShardsAndReplicas()) {
            final int numberOfShards = numberOfShards();
            if (numberOfShards > 0) {
                builder.put(SETTING_NUMBER_OF_SHARDS, numberOfShards).build();
            }
            final int numberOfReplicas = numberOfReplicas();
            if (numberOfReplicas >= 0) {
                builder.put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas).build();
            }
        }
        // 30% of the time
        if (useCustomDataPath() && (randomInt(9) < 3)) {
            final String dataPath = "data/custom-" + CHILD_JVM_ID + "/" + UUID.randomUUID().toString();
            logger.info("using custom data_path for index: [{}]", dataPath);
            builder.put(IndexMetaData.SETTING_DATA_PATH, dataPath);
        }
        return builder.build();
    }

    /**
     * Creates one or more indices and asserts that the indices are acknowledged. If one of the indices
     * already exists this method will fail and wipe all the indices created so far.
     */
    public final void createIndex(final String... names) {

        final List<String> created = new ArrayList<>();
        for (final String name : names) {
            boolean success = false;
            try {
                assertAcked(prepareCreate(name));
                created.add(name);
                success = true;
            } finally {
                if (!success && !created.isEmpty()) {
                    cluster().wipeIndices(created.toArray(new String[created.size()]));
                }
            }
        }
    }

    /**
     * Creates a new {@link CreateIndexRequestBuilder} with the settings obtained from {@link #indexSettings()}.
     */
    public final CreateIndexRequestBuilder prepareCreate(final String index) {
        return client().admin().indices().prepareCreate(index).setSettings(indexSettings());
    }

    /**
     * Creates a new {@link CreateIndexRequestBuilder} with the settings obtained from {@link #indexSettings()}.
     * The index that is created with this builder will only be allowed to allocate on the number of nodes passed to this
     * method.
     * <p>
     * This method uses allocation deciders to filter out certain nodes to allocate the created index on. It defines allocation rules based
     * on <code>index.routing.allocation.exclude._name</code>.
     * </p>
     */
    public final CreateIndexRequestBuilder prepareCreate(final String index, final int numNodes) {
        return prepareCreate(index, numNodes, ImmutableSettings.builder());
    }

    /**
     * Creates a new {@link CreateIndexRequestBuilder} with the settings obtained from {@link #indexSettings()}.
     * The index that is created with this builder will only be allowed to allocate on the number of nodes passed to this
     * method.
     * <p>
     * This method uses allocation deciders to filter out certain nodes to allocate the created index on. It defines allocation rules based
     * on <code>index.routing.allocation.exclude._name</code>.
     * </p>
     */
    public CreateIndexRequestBuilder prepareCreate(final String index, final int numNodes, final ImmutableSettings.Builder settingsBuilder) {
        internalCluster().ensureAtLeastNumDataNodes(numNodes);

        final ImmutableSettings.Builder builder = ImmutableSettings.builder().put(indexSettings()).put(settingsBuilder.build());

        if (numNodes > 0) {
            getExcludeSettings(index, numNodes, builder);
        }
        return client().admin().indices().prepareCreate(index).setSettings(builder.build());
    }

    private ImmutableSettings.Builder getExcludeSettings(final String index, final int num, final ImmutableSettings.Builder builder) {
        final String exclude = Joiner.on(',').join(internalCluster().allDataNodesButN(num));
        builder.put("index.routing.allocation.exclude._name", exclude);
        return builder;
    }

    /**
     * Waits until all nodes have no pending tasks.
     */
    public void waitNoPendingTasksOnAll() throws Exception {
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get());
        assertBusy(new Runnable() {
            @Override
            public void run() {
                for (final Client client : clients()) {
                    final PendingClusterTasksResponse pendingTasks = client.admin().cluster().preparePendingClusterTasks().setLocal(true)
                            .get();
                    assertThat("client " + client + " still has pending tasks " + pendingTasks.prettyPrint(), pendingTasks,
                            Matchers.emptyIterable());
                }
            }
        });
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get());
    }

    /**
     * Waits until the elected master node has no pending tasks.
     */
    public void waitNoPendingTasksOnMaster() throws Exception {
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get());
        assertBusy(new Runnable() {
            @Override
            public void run() {
                final PendingClusterTasksResponse pendingTasks = client().admin().cluster().preparePendingClusterTasks().setLocal(true)
                        .get();
                assertThat("master still has pending tasks " + pendingTasks.prettyPrint(), pendingTasks, Matchers.emptyIterable());
            }
        });
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get());
    }

    /**
     * Waits till a (pattern) field name mappings concretely exists on all nodes. Note, this waits for the current
     * started shards and checks for concrete mappings.
     */
    public void waitForConcreteMappingsOnAll(final String index, final String type, final String... fieldNames) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                final Set<String> nodes = internalCluster().nodesInclude(index);
                assertThat(nodes, Matchers.not(Matchers.emptyIterable()));
                for (final String node : nodes) {
                    final IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
                    final IndexService indexService = indicesService.indexService(index);
                    assertThat("index service doesn't exists on " + node, indexService, notNullValue());
                    final DocumentMapper documentMapper = indexService.mapperService().documentMapper(type);
                    assertThat("document mapper doesn't exists on " + node, documentMapper, notNullValue());
                    for (final String fieldName : fieldNames) {
                        final List<String> matches = documentMapper.mappers().simpleMatchToFullName(fieldName);
                        assertThat("field " + fieldName + " doesn't exists on " + node, matches, Matchers.not(emptyIterable()));
                    }
                }
            }
        });
        waitForMappingOnMaster(index, type, fieldNames);
    }

    /**
     * Waits for the given mapping type to exists on the master node.
     */
    public void waitForMappingOnMaster(final String index, final String type, final String... fieldNames) throws Exception {
        assertBusy(new Callable() {
            @Override
            public Object call() throws Exception {
                final GetMappingsResponse response = client().admin().indices().prepareGetMappings(index).setTypes(type).get();
                final ImmutableOpenMap<String, MappingMetaData> mappings = response.getMappings().get(index);
                assertThat(mappings, notNullValue());
                final MappingMetaData mappingMetaData = mappings.get(type);
                assertThat(mappingMetaData, notNullValue());

                final Map<String, Object> mappingSource = mappingMetaData.getSourceAsMap();
                assertFalse(mappingSource.isEmpty());
                assertTrue(mappingSource.containsKey("properties"));

                for (String fieldName : fieldNames) {
                    final Map<String, Object> mappingProperties = (Map<String, Object>) mappingSource.get("properties");
                    if (fieldName.indexOf('.') != -1) {
                        fieldName = fieldName.replace(".", ".properties.");
                    }
                    assertThat("field " + fieldName + " doesn't exists in mapping " + mappingMetaData.source().string(),
                            XContentMapValues.extractValue(fieldName, mappingProperties), notNullValue());
                }

                return null;
            }
        });
    }

    /**
     * Restricts the given index to be allocated on <code>n</code> nodes using the allocation deciders.
     * Yet if the shards can't be allocated on any other node shards for this index will remain allocated on
     * more than <code>n</code> nodes.
     */
    public void allowNodes(final String index, final int n) {
        assert index != null;
        internalCluster().ensureAtLeastNumDataNodes(n);
        final ImmutableSettings.Builder builder = ImmutableSettings.builder();
        if (n > 0) {
            getExcludeSettings(index, n, builder);
        }
        final Settings build = builder.build();
        if (!build.getAsMap().isEmpty()) {
            logger.debug("allowNodes: updating [{}]'s setting to [{}]", index, build.toDelimitedString(';'));
            client().admin().indices().prepareUpdateSettings(index).setSettings(build).execute().actionGet();
        }
    }

    /**
     * Ensures the cluster has a green state via the cluster health API. This method will also wait for relocations.
     * It is useful to ensure that all action on the cluster have finished and all shards that were currently relocating
     * are now allocated and started.
     */
    public ClusterHealthStatus ensureGreen(final String... indices) {
        return ensureGreen(TimeValue.timeValueSeconds(30), indices);
    }

    /**
     * Ensures the cluster has a green state via the cluster health API. This method will also wait for relocations.
     * It is useful to ensure that all action on the cluster have finished and all shards that were currently relocating
     * are now allocated and started.
     *
     * @param timeout
     *            time out value to set on {@link org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest}
     */
    public ClusterHealthStatus ensureGreen(final TimeValue timeout, final String... indices) {
        final ClusterHealthResponse actionGet = client()
                .admin()
                .cluster()
                .health(Requests.clusterHealthRequest(indices).timeout(timeout).waitForGreenStatus().waitForEvents(Priority.LANGUID)
                        .waitForRelocatingShards(0)).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureGreen timed out, cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState()
                    .prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            assertThat("timed out waiting for green state", actionGet.isTimedOut(), equalTo(false));
        }
        assertThat(actionGet.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        logger.debug("indices {} are green", indices.length == 0 ? "[_all]" : indices);
        return actionGet.getStatus();
    }

    /**
     * Waits for all relocating shards to become active using the cluster health API.
     */
    public ClusterHealthStatus waitForRelocation() {
        return waitForRelocation(null);
    }

    /**
     * Waits for all relocating shards to become active and the cluster has reached the given health status
     * using the cluster health API.
     */
    public ClusterHealthStatus waitForRelocation(final ClusterHealthStatus status) {
        final ClusterHealthRequest request = Requests.clusterHealthRequest().waitForRelocatingShards(0);
        if (status != null) {
            request.waitForStatus(status);
        }
        final ClusterHealthResponse actionGet = client().admin().cluster().health(request).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("waitForRelocation timed out (status={}), cluster state:\n{}\n{}", status, client().admin().cluster()
                    .prepareState().get().getState().prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get()
                    .prettyPrint());
            assertThat("timed out waiting for relocation", actionGet.isTimedOut(), equalTo(false));
        }
        if (status != null) {
            assertThat(actionGet.getStatus(), equalTo(status));
        }
        return actionGet.getStatus();
    }

    /**
     * Waits until at least a give number of document is visible for searchers
     *
     * @param numDocs
     *            number of documents to wait for.
     * @return the actual number of docs seen.
     * @throws InterruptedException
     */
    public long waitForDocs(final long numDocs) throws InterruptedException {
        return waitForDocs(numDocs, null);
    }

    /**
     * Waits until at least a give number of document is visible for searchers
     *
     * @param numDocs
     *            number of documents to wait for
     * @param indexer
     *            a {@link org.elasticsearch.test.BackgroundIndexer}. If supplied it will be first checked for documents indexed.
     *            This saves on unneeded searches.
     * @return the actual number of docs seen.
     * @throws InterruptedException
     */
    public long waitForDocs(final long numDocs, final @Nullable BackgroundIndexer indexer) throws InterruptedException {
        // indexing threads can wait for up to ~1m before retrying when they first try to index into a shard which is not STARTED.
        return waitForDocs(numDocs, 90, TimeUnit.SECONDS, indexer);
    }

    /**
     * Waits until at least a give number of document is visible for searchers
     *
     * @param numDocs
     *            number of documents to wait for
     * @param maxWaitTime
     *            if not progress have been made during this time, fail the test
     * @param maxWaitTimeUnit
     *            the unit in which maxWaitTime is specified
     * @param indexer
     *            a {@link org.elasticsearch.test.BackgroundIndexer}. If supplied it will be first checked for documents indexed.
     *            This saves on unneeded searches.
     * @return the actual number of docs seen.
     * @throws InterruptedException
     */
    public long waitForDocs(final long numDocs, final int maxWaitTime, final TimeUnit maxWaitTimeUnit,
            final @Nullable BackgroundIndexer indexer) throws InterruptedException {
        final AtomicLong lastKnownCount = new AtomicLong(-1);
        long lastStartCount = -1;
        final Predicate<Object> testDocs = new Predicate<Object>() {
            @Override
            public boolean apply(final Object o) {
                if (indexer != null) {
                    lastKnownCount.set(indexer.totalIndexedDocs());
                }
                if (lastKnownCount.get() >= numDocs) {
                    final long count = client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount();
                    if (count == lastKnownCount.get()) {
                        // no progress - try to refresh for the next time
                        client().admin().indices().prepareRefresh().get();
                    }
                    lastKnownCount.set(count);
                    logger.debug("[{}] docs visible for search. waiting for [{}]", lastKnownCount.get(), numDocs);
                } else {
                    logger.debug("[{}] docs indexed. waiting for [{}]", lastKnownCount.get(), numDocs);
                }
                return lastKnownCount.get() >= numDocs;
            }
        };

        while (!awaitBusy(testDocs, maxWaitTime, maxWaitTimeUnit)) {
            if (lastStartCount == lastKnownCount.get()) {
                // we didn't make any progress
                fail("failed to reach " + numDocs + "docs");
            }
            lastStartCount = lastKnownCount.get();
        }
        return lastKnownCount.get();
    }

    /**
     * Sets the cluster's minimum master node and make sure the response is acknowledge.
     * Note: this doesn't guaranty the new settings is in effect, just that it has been received bu all nodes.
     */
    public void setMinimumMasterNodes(final int n) {
        assertTrue(client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(settingsBuilder().put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, n)).get()
                .isAcknowledged());
    }

    /**
     * Ensures the cluster has a yellow state via the cluster health API.
     */
    public ClusterHealthStatus ensureYellow(final String... indices) {
        final ClusterHealthResponse actionGet = client()
                .admin()
                .cluster()
                .health(Requests.clusterHealthRequest(indices).waitForRelocatingShards(0).waitForYellowStatus()
                        .waitForEvents(Priority.LANGUID)).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureYellow timed out, cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState()
                    .prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            assertThat("timed out waiting for yellow", actionGet.isTimedOut(), equalTo(false));
        }
        logger.debug("indices {} are yellow", indices.length == 0 ? "[_all]" : indices);
        return actionGet.getStatus();
    }

    /**
     * Prints the current cluster state as debug logging.
     */
    public void logClusterState() {
        logger.debug("cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState().prettyPrint(), client().admin()
                .cluster().preparePendingClusterTasks().get().prettyPrint());
    }

    /**
     * Prints the segments info for the given indices as debug logging.
     */
    public void logSegmentsState(final String... indices) throws Exception {
        final IndicesSegmentResponse segsRsp = client().admin().indices().prepareSegments(indices).get();
        logger.debug("segments {} state: \n{}", indices.length == 0 ? "[_all]" : indices,
                segsRsp.toXContent(JsonXContent.contentBuilder().prettyPrint(), ToXContent.EMPTY_PARAMS).string());
    }

    /**
     * Prints current memory stats as info logging.
     */
    public void logMemoryStats() {
        logger.info("memory: {}", XContentHelper.toString(client().admin().cluster().prepareNodesStats().clear().setJvm(true).get()));
    }

    void ensureClusterSizeConsistency() {
        if (cluster() != null) { // if static init fails the cluster can be null
            logger.trace("Check consistency for [{}] nodes", cluster().size());
            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(cluster().size())).get());
        }
    }

    /**
     * Ensures the cluster is in a searchable state for the given indices. This means a searchable copy of each
     * shard is available on the cluster.
     */
    protected ClusterHealthStatus ensureSearchable(final String... indices) {
        // this is just a temporary thing but it's easier to change if it is encapsulated.
        return ensureGreen(indices);
    }

    /**
     * Syntactic sugar for:
     * 
     * <pre>
     * client().prepareIndex(index, type).setSource(source).execute().actionGet();
     * </pre>
     */
    protected final IndexResponse index(final String index, final String type, final XContentBuilder source) {
        return client().prepareIndex(index, type).setSource(source).execute().actionGet();
    }

    /**
     * Syntactic sugar for:
     * 
     * <pre>
     * client().prepareIndex(index, type).setSource(source).execute().actionGet();
     * </pre>
     */
    protected final IndexResponse index(final String index, final String type, final String id, final Map<String, Object> source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }

    /**
     * Syntactic sugar for:
     * 
     * <pre>
     * client().prepareGet(index, type, id).execute().actionGet();
     * </pre>
     */
    protected final GetResponse get(final String index, final String type, final String id) {
        return client().prepareGet(index, type, id).execute().actionGet();
    }

    /**
     * Syntactic sugar for:
     * 
     * <pre>
     * return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
     * </pre>
     */
    protected final IndexResponse index(final String index, final String type, final String id, final XContentBuilder source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }

    /**
     * Syntactic sugar for:
     * 
     * <pre>
     * return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
     * </pre>
     */
    protected final IndexResponse index(final String index, final String type, final String id, final Object... source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }

    /**
     * Syntactic sugar for:
     * <p/>
     * 
     * <pre>
     * return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
     * </pre>
     * <p/>
     * where source is a String.
     */
    protected final IndexResponse index(final String index, final String type, final String id, final String source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }

    /**
     * Waits for relocations and refreshes all indices in the cluster.
     *
     * @see #waitForRelocation()
     */
    protected final RefreshResponse refresh() {
        waitForRelocation();
        // TODO RANDOMIZE with flush?
        final RefreshResponse actionGet = client().admin().indices().prepareRefresh().execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    /**
     * Flushes and refreshes all indices in the cluster
     */
    protected final void flushAndRefresh(final String... indices) {
        flush(indices);
        refresh();
    }

    /**
     * Flush some or all indices in the cluster.
     */
    protected final FlushResponse flush(final String... indices) {
        waitForRelocation();
        final FlushResponse actionGet = client().admin().indices().prepareFlush(indices).setWaitIfOngoing(true).execute().actionGet();
        for (final ShardOperationFailedException failure : actionGet.getShardFailures()) {
            assertThat("unexpected flush failure " + failure.reason(), failure.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }
        return actionGet;
    }

    /**
     * Waits for all relocations and optimized all indices in the cluster to 1 segment.
     */
    protected OptimizeResponse optimize() {
        waitForRelocation();
        final OptimizeResponse actionGet = client().admin().indices().prepareOptimize().setMaxNumSegments(1).setForce(randomBoolean())
                .execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    /**
     * Returns <code>true</code> iff the given index exists otherwise <code>false</code>
     */
    protected boolean indexExists(final String index) {
        final IndicesExistsResponse actionGet = client().admin().indices().prepareExists(index).execute().actionGet();
        return actionGet.isExists();
    }

    /**
     * Returns a random admin client. This client can either be a node or a transport client pointing to any of
     * the nodes in the cluster.
     */
    protected AdminClient admin() {
        return client().admin();
    }

    /**
     * Convenience method that forwards to {@link #indexRandom(boolean, List)}.
     */
    public void indexRandom(final boolean forceRefresh, final IndexRequestBuilder... builders) throws InterruptedException,
    ExecutionException {
        indexRandom(forceRefresh, Arrays.asList(builders));
    }

    public void indexRandom(final boolean forceRefresh, final boolean dummyDocuments, final IndexRequestBuilder... builders)
            throws InterruptedException, ExecutionException {
        indexRandom(forceRefresh, dummyDocuments, Arrays.asList(builders));
    }

    private static final String RANDOM_BOGUS_TYPE = "RANDOM_BOGUS_TYPE______";

    /**
     * Indexes the given {@link IndexRequestBuilder} instances randomly. It shuffles the given builders and either
     * indexes them in a blocking or async fashion. This is very useful to catch problems that relate to internal document
     * ids or index segment creations. Some features might have bug when a given document is the first or the last in a
     * segment or if only one document is in a segment etc. This method prevents issues like this by randomizing the index
     * layout.
     *
     * @param forceRefresh
     *            if <tt>true</tt> all involved indices are refreshed once the documents are indexed. Additionally if <tt>true</tt> some
     *            empty dummy documents are may be randomly inserted into the document list and deleted once all documents are indexed.
     *            This is useful to produce deleted documents on the server side.
     * @param builders
     *            the documents to index.
     * @see #indexRandom(boolean, boolean, java.util.List)
     */
    public void indexRandom(final boolean forceRefresh, final List<IndexRequestBuilder> builders) throws InterruptedException,
    ExecutionException {
        indexRandom(forceRefresh, forceRefresh, builders);
    }

    /**
     * Indexes the given {@link IndexRequestBuilder} instances randomly. It shuffles the given builders and either
     * indexes they in a blocking or async fashion. This is very useful to catch problems that relate to internal document
     * ids or index segment creations. Some features might have bug when a given document is the first or the last in a
     * segment or if only one document is in a segment etc. This method prevents issues like this by randomizing the index
     * layout.
     *
     * @param forceRefresh
     *            if <tt>true</tt> all involved indices are refreshed once the documents are indexed.
     * @param dummyDocuments
     *            if <tt>true</tt> some empty dummy documents may be randomly inserted into the document list and deleted once
     *            all documents are indexed. This is useful to produce deleted documents on the server side.
     * @param builders
     *            the documents to index.
     */
    public void indexRandom(final boolean forceRefresh, final boolean dummyDocuments, final List<IndexRequestBuilder> builders)
            throws InterruptedException, ExecutionException {
        indexRandom(forceRefresh, dummyDocuments, true, builders);
    }

    /**
     * Indexes the given {@link IndexRequestBuilder} instances randomly. It shuffles the given builders and either
     * indexes they in a blocking or async fashion. This is very useful to catch problems that relate to internal document
     * ids or index segment creations. Some features might have bug when a given document is the first or the last in a
     * segment or if only one document is in a segment etc. This method prevents issues like this by randomizing the index
     * layout.
     *
     * @param forceRefresh
     *            if <tt>true</tt> all involved indices are refreshed once the documents are indexed.
     * @param dummyDocuments
     *            if <tt>true</tt> some empty dummy documents may be randomly inserted into the document list and deleted once
     *            all documents are indexed. This is useful to produce deleted documents on the server side.
     * @param maybeFlush
     *            if <tt>true</tt> this method may randomly execute full flushes after index operations.
     * @param builders
     *            the documents to index.
     */
    public void indexRandom(final boolean forceRefresh, final boolean dummyDocuments, final boolean maybeFlush,
            List<IndexRequestBuilder> builders) throws InterruptedException, ExecutionException {

        final Random random = getRandom();
        final Set<String> indicesSet = new HashSet<>();
        for (final IndexRequestBuilder builder : builders) {
            indicesSet.add(builder.request().index());
        }
        final Set<Tuple<String, String>> bogusIds = new HashSet<>();
        if (random.nextBoolean() && !builders.isEmpty() && dummyDocuments) {
            builders = new ArrayList<>(builders);
            final String[] indices = indicesSet.toArray(new String[indicesSet.size()]);
            // inject some bogus docs
            final int numBogusDocs = scaledRandomIntBetween(1, builders.size() * 2);
            final int unicodeLen = between(1, 10);
            for (int i = 0; i < numBogusDocs; i++) {
                final String id = randomRealisticUnicodeOfLength(unicodeLen) + Integer.toString(dummmyDocIdGenerator.incrementAndGet());
                final String index = RandomPicks.randomFrom(random, indices);
                bogusIds.add(new Tuple<>(index, id));
                builders.add(client().prepareIndex(index, RANDOM_BOGUS_TYPE, id).setSource("{}"));
            }
        }
        final String[] indices = indicesSet.toArray(new String[indicesSet.size()]);
        Collections.shuffle(builders, random);
        final CopyOnWriteArrayList<Tuple<IndexRequestBuilder, Throwable>> errors = new CopyOnWriteArrayList<>();
        final List<CountDownLatch> inFlightAsyncOperations = new ArrayList<>();
        // If you are indexing just a few documents then frequently do it one at a time.  If many then frequently in bulk.
        if (builders.size() < FREQUENT_BULK_THRESHOLD ? frequently() : builders.size() < ALWAYS_BULK_THRESHOLD ? rarely() : false) {
            if (frequently()) {
                logger.info("Index [{}] docs async: [{}] bulk: [{}]", builders.size(), true, false);
                for (final IndexRequestBuilder indexRequestBuilder : builders) {
                    indexRequestBuilder.execute(new PayloadLatchedActionListener<IndexResponse, IndexRequestBuilder>(indexRequestBuilder,
                            newLatch(inFlightAsyncOperations), errors));
                    postIndexAsyncActions(indices, inFlightAsyncOperations, maybeFlush);
                }
            } else {
                logger.info("Index [{}] docs async: [{}] bulk: [{}]", builders.size(), false, false);
                for (final IndexRequestBuilder indexRequestBuilder : builders) {
                    indexRequestBuilder.execute().actionGet();
                    postIndexAsyncActions(indices, inFlightAsyncOperations, maybeFlush);
                }
            }
        } else {
            final List<List<IndexRequestBuilder>> partition = Lists.partition(builders,
                    Math.min(MAX_BULK_INDEX_REQUEST_SIZE, Math.max(1, (int) (builders.size() * randomDouble()))));
            logger.info("Index [{}] docs async: [{}] bulk: [{}] partitions [{}]", builders.size(), false, true, partition.size());
            for (final List<IndexRequestBuilder> segmented : partition) {
                final BulkRequestBuilder bulkBuilder = client().prepareBulk();
                for (final IndexRequestBuilder indexRequestBuilder : segmented) {
                    bulkBuilder.add(indexRequestBuilder);
                }
                final BulkResponse actionGet = bulkBuilder.execute().actionGet();
                assertThat(actionGet.hasFailures() ? actionGet.buildFailureMessage() : "", actionGet.hasFailures(), equalTo(false));
            }
        }
        for (final CountDownLatch operation : inFlightAsyncOperations) {
            operation.await();
        }
        final List<Throwable> actualErrors = new ArrayList<>();
        for (final Tuple<IndexRequestBuilder, Throwable> tuple : errors) {
            if (ExceptionsHelper.unwrapCause(tuple.v2()) instanceof EsRejectedExecutionException) {
                tuple.v1().execute().actionGet(); // re-index if rejected
            } else {
                actualErrors.add(tuple.v2());
            }
        }
        assertThat(actualErrors, emptyIterable());
        if (!bogusIds.isEmpty()) {
            // delete the bogus types again - it might trigger merges or at least holes in the segments and enforces deleted docs!
            for (final Tuple<String, String> doc : bogusIds) {
                // see https://github.com/elasticsearch/elasticsearch/issues/8706
                final DeleteResponse deleteResponse = client().prepareDelete(doc.v1(), RANDOM_BOGUS_TYPE, doc.v2()).get();
                if (deleteResponse.isFound() == false) {
                    logger.warn("failed to delete a dummy doc [{}][{}]", doc.v1(), doc.v2());
                }
            }
        }
        if (forceRefresh) {
            assertNoFailures(client().admin().indices().prepareRefresh(indices).setIndicesOptions(IndicesOptions.lenientExpandOpen())
                    .execute().get());
        }
    }

    private final AtomicInteger dummmyDocIdGenerator = new AtomicInteger();

    /** Disables translog flushing for the specified index */
    public static void disableTranslogFlush(final String index) {
        final Settings settings = ImmutableSettings.builder().put(TranslogService.INDEX_TRANSLOG_DISABLE_FLUSH, true).build();
        client().admin().indices().prepareUpdateSettings(index).setSettings(settings).get();
    }

    /** Enables translog flushing for the specified index */
    public static void enableTranslogFlush(final String index) {
        final Settings settings = ImmutableSettings.builder().put(TranslogService.INDEX_TRANSLOG_DISABLE_FLUSH, false).build();
        client().admin().indices().prepareUpdateSettings(index).setSettings(settings).get();
    }

    private static CountDownLatch newLatch(final List<CountDownLatch> latches) {
        final CountDownLatch l = new CountDownLatch(1);
        latches.add(l);
        return l;
    }

    /**
     * Maybe refresh, optimize, or flush then always make sure there aren't too many in flight async operations.
     */
    private void postIndexAsyncActions(final String[] indices, final List<CountDownLatch> inFlightAsyncOperations, final boolean maybeFlush)
            throws InterruptedException {
        if (rarely()) {
            if (rarely()) {
                client().admin().indices().prepareRefresh(indices).setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .execute(new LatchedActionListener<RefreshResponse>(newLatch(inFlightAsyncOperations)));
            } else if (maybeFlush && rarely()) {
                client().admin().indices().prepareFlush(indices).setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .execute(new LatchedActionListener<FlushResponse>(newLatch(inFlightAsyncOperations)));
            } else if (rarely()) {
                client().admin().indices().prepareOptimize(indices).setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setMaxNumSegments(between(1, 10)).setFlush(maybeFlush && randomBoolean())
                .execute(new LatchedActionListener<OptimizeResponse>(newLatch(inFlightAsyncOperations)));
            }
        }
        while (inFlightAsyncOperations.size() > MAX_IN_FLIGHT_ASYNC_INDEXES) {
            final int waitFor = between(0, inFlightAsyncOperations.size() - 1);
            inFlightAsyncOperations.remove(waitFor).await();
        }
    }

    /**
     * The scope of a test cluster used together with {@link org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope} annotations
     * on {@link org.elasticsearch.test.ElasticsearchIntegrationTest} subclasses.
     */
    public static enum Scope {
        /**
         * A cluster shared across all method in a single test suite
         */
        SUITE,
        /**
         * A test exclusive test cluster
         */
        TEST
    }

    /**
     * Defines a cluster scope for a {@link org.elasticsearch.test.ElasticsearchIntegrationTest} subclass.
     * By default if no {@link ClusterScope} annotation is present {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#SUITE}
     * is used
     * together with randomly chosen settings like number of nodes etc.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.TYPE })
    public @interface ClusterScope {
        /**
         * Returns the scope. {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#SUITE} is default.
         */
        Scope scope() default Scope.SUITE;

        /**
         * Returns the number of nodes in the cluster. Default is <tt>-1</tt> which means
         * a random number of nodes is used, where the minimum and maximum number of nodes
         * are either the specified ones or the default ones if not specified.
         */
        int numDataNodes() default -1;

        /**
         * Returns the minimum number of nodes in the cluster. Default is {@link InternalTestCluster#DEFAULT_MIN_NUM_DATA_NODES}.
         * Ignored when {@link ClusterScope#numDataNodes()} is set.
         */
        int minNumDataNodes() default InternalTestCluster.DEFAULT_MIN_NUM_DATA_NODES;

        /**
         * Returns the maximum number of nodes in the cluster. Default is {@link InternalTestCluster#DEFAULT_MAX_NUM_DATA_NODES}.
         * Ignored when {@link ClusterScope#numDataNodes()} is set.
         */
        int maxNumDataNodes() default InternalTestCluster.DEFAULT_MAX_NUM_DATA_NODES;

        /**
         * Returns the number of client nodes in the cluster. Default is {@link InternalTestCluster#DEFAULT_NUM_CLIENT_NODES}, a
         * negative value means that the number of client nodes will be randomized.
         */
        int numClientNodes() default InternalTestCluster.DEFAULT_NUM_CLIENT_NODES;

        /**
         * Returns the transport client ratio. By default this returns <code>-1</code> which means a random
         * ratio in the interval <code>[0..1]</code> is used.
         */
        double transportClientRatio() default -1;

        /**
         * Return whether or not to enable dynamic templates for the mappings.
         */
        boolean randomDynamicTemplates() default true;
    }

    private class LatchedActionListener<Response> implements ActionListener<Response> {
        private final CountDownLatch latch;

        public LatchedActionListener(final CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public final void onResponse(final Response response) {
            latch.countDown();
        }

        @Override
        public final void onFailure(final Throwable t) {
            try {
                logger.info("Action Failed", t);
                addError(t);
            } finally {
                latch.countDown();
            }
        }

        protected void addError(final Throwable t) {
        }

    }

    private class PayloadLatchedActionListener<Response, T> extends LatchedActionListener<Response> {
        private final CopyOnWriteArrayList<Tuple<T, Throwable>> errors;
        private final T builder;

        public PayloadLatchedActionListener(final T builder, final CountDownLatch latch,
                final CopyOnWriteArrayList<Tuple<T, Throwable>> errors) {
            super(latch);
            this.errors = errors;
            this.builder = builder;
        }

        @Override
        protected void addError(final Throwable t) {
            errors.add(new Tuple<>(builder, t));
        }

    }

    /**
     * Clears the given scroll Ids
     */
    public void clearScroll(final String... scrollIds) {
        final ClearScrollResponse clearResponse = client().prepareClearScroll().setScrollIds(Arrays.asList(scrollIds)).get();
        assertThat(clearResponse.isSucceeded(), equalTo(true));
    }

    private static ClusterScope getAnnotation(final Class<?> clazz) {
        if (clazz == Object.class || clazz == ElasticsearchIntegrationTest.class) {
            return null;
        }
        final ClusterScope annotation = clazz.getAnnotation(ClusterScope.class);
        if (annotation != null) {
            return annotation;
        }
        return getAnnotation(clazz.getSuperclass());
    }

    private Scope getCurrentClusterScope() {
        return getCurrentClusterScope(this.getClass());
    }

    private static Scope getCurrentClusterScope(final Class<?> clazz) {
        final ClusterScope annotation = getAnnotation(clazz);
        // if we are not annotated assume suite!
        return annotation == null ? Scope.SUITE : annotation.scope();
    }

    private int getNumDataNodes() {
        final ClusterScope annotation = getAnnotation(this.getClass());
        return annotation == null ? -1 : annotation.numDataNodes();
    }

    private int getMinNumDataNodes() {
        final ClusterScope annotation = getAnnotation(this.getClass());
        return annotation == null ? InternalTestCluster.DEFAULT_MIN_NUM_DATA_NODES : annotation.minNumDataNodes();
    }

    private int getMaxNumDataNodes() {
        final ClusterScope annotation = getAnnotation(this.getClass());
        return annotation == null ? InternalTestCluster.DEFAULT_MAX_NUM_DATA_NODES : annotation.maxNumDataNodes();
    }

    private int getNumClientNodes() {
        final ClusterScope annotation = getAnnotation(this.getClass());
        return annotation == null ? InternalTestCluster.DEFAULT_NUM_CLIENT_NODES : annotation.numClientNodes();
    }

    private boolean randomDynamicTemplates() {
        final ClusterScope annotation = getAnnotation(this.getClass());
        return annotation == null || annotation.randomDynamicTemplates();
    }

    /**
     * This method is used to obtain settings for the <tt>Nth</tt> node in the cluster.
     * Nodes in this cluster are associated with an ordinal number such that nodes can
     * be started with specific configurations. This method might be called multiple
     * times with the same ordinal and is expected to return the same value for each invocation.
     * In other words subclasses must ensure this method is idempotent.
     */
    protected Settings nodeSettings(final int nodeOrdinal) {
        return settingsBuilder()
                // Default the watermarks to absurdly low to prevent the tests
                // from failing on nodes without enough disk space
                .put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, "1b")
                .put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, "1b").build();
    }

    /**
     * This method is used to obtain additional settings for clients created by the internal cluster.
     * These settings will be applied on the client in addition to some randomized settings defined in
     * the cluster. These setttings will also override any other settings the internal cluster might
     * add by default.
     */
    protected Settings transportClientSettings() {
        return ImmutableSettings.EMPTY;
    }

    private ExternalTestCluster buildExternalCluster(final String clusterAddresses) {
        final String[] stringAddresses = clusterAddresses.split(",");
        final TransportAddress[] transportAddresses = new TransportAddress[stringAddresses.length];
        int i = 0;
        for (final String stringAddress : stringAddresses) {
            final String[] split = stringAddress.split(":");
            if (split.length < 2) {
                throw new IllegalArgumentException("address [" + clusterAddresses + "] not valid");
            }
            try {
                transportAddresses[i++] = new InetSocketTransportAddress(split[0], Integer.valueOf(split[1]));
            } catch (final NumberFormatException e) {
                throw new IllegalArgumentException("port is not valid, expected number but was [" + split[1] + "]");
            }
        }
        return new ExternalTestCluster(transportAddresses);
    }

    protected TestCluster buildTestCluster(final Scope scope, final long seed) throws IOException {
        final String clusterAddresses = System.getProperty(TESTS_CLUSTER);
        if (Strings.hasLength(clusterAddresses)) {
            if (scope == Scope.TEST) {
                throw new IllegalArgumentException("Cannot run TEST scope test with " + TESTS_CLUSTER);
            }
            return buildExternalCluster(clusterAddresses);
        }

        final String nodePrefix;
        switch (scope) {
            case TEST:
                nodePrefix = TEST_CLUSTER_NODE_PREFIX;
                break;
            case SUITE:
                nodePrefix = SUITE_CLUSTER_NODE_PREFIX;
                break;
            default:
                throw new ElasticsearchException("Scope not supported: " + scope);
        }
        final SettingsSource settingsSource = new SettingsSource() {
            @Override
            public Settings node(final int nodeOrdinal) {
                return ImmutableSettings.builder().put(InternalNode.HTTP_ENABLED, false).put(nodeSettings(nodeOrdinal)).build();
            }

            @Override
            public Settings transportClient() {
                return transportClientSettings();
            }
        };

        final int numDataNodes = getNumDataNodes();
        int minNumDataNodes;
        int maxNumDataNodes;
        if (numDataNodes >= 0) {
            minNumDataNodes = maxNumDataNodes = numDataNodes;
        } else {
            minNumDataNodes = getMinNumDataNodes();
            maxNumDataNodes = getMaxNumDataNodes();
        }

        return new InternalTestCluster(seed, minNumDataNodes, maxNumDataNodes, clusterName(scope.name(), Integer.toString(CHILD_JVM_ID),
                seed), settingsSource, getNumClientNodes(), InternalTestCluster.DEFAULT_ENABLE_HTTP_PIPELINING, CHILD_JVM_ID, nodePrefix);
    }

    /**
     * Returns the client ratio configured via
     */
    private static double transportClientRatio() {
        final String property = System.getProperty(TESTS_CLIENT_RATIO);
        if (property == null || property.isEmpty()) {
            return Double.NaN;
        }
        return Double.parseDouble(property);
    }

    /**
     * Returns the transport client ratio from the class level annotation or via {@link System#getProperty(String)} if available. If both
     * are not available this will
     * return a random ratio in the interval <tt>[0..1]</tt>
     */
    protected double getPerTestTransportClientRatio() {
        final ClusterScope annotation = getAnnotation(this.getClass());
        double perTestRatio = -1;
        if (annotation != null) {
            perTestRatio = annotation.transportClientRatio();
        }
        if (perTestRatio == -1) {
            return Double.isNaN(TRANSPORT_CLIENT_RATIO) ? randomDouble() : TRANSPORT_CLIENT_RATIO;
        }
        assert perTestRatio >= 0.0 && perTestRatio <= 1.0;
        return perTestRatio;
    }

    /**
     * Returns a random numeric field data format from the choices of "array" or "doc_values".
     */
    public static String randomNumericFieldDataFormat() {
        return randomFrom(Arrays.asList("array", "doc_values"));
    }

    /**
     * Returns a random bytes field data format from the choices of
     * "paged_bytes", "fst", or "doc_values".
     */
    public static String randomBytesFieldDataFormat() {
        return randomFrom(Arrays.asList("paged_bytes", "fst", "doc_values"));
    }

    /**
     * Returns a random JODA Time Zone based on Java Time Zones
     */
    public static DateTimeZone randomDateTimeZone() {
        DateTimeZone timeZone;

        // It sounds like some Java Time Zones are unknown by JODA. For example: Asia/Riyadh88
        // We need to fallback in that case to a known time zone
        try {
            timeZone = DateTimeZone.forTimeZone(randomTimeZone());
        } catch (final IllegalArgumentException e) {
            timeZone = DateTimeZone.forOffsetHours(randomIntBetween(-12, 12));
        }

        return timeZone;
    }

    protected NumShards getNumShards(final String index) {
        final MetaData metaData = client().admin().cluster().prepareState().get().getState().metaData();
        assertThat(metaData.hasIndex(index), equalTo(true));
        final int numShards = Integer.valueOf(metaData.index(index).settings().get(SETTING_NUMBER_OF_SHARDS));
        final int numReplicas = Integer.valueOf(metaData.index(index).settings().get(SETTING_NUMBER_OF_REPLICAS));
        return new NumShards(numShards, numReplicas);
    }

    /**
     * Asserts that all shards are allocated on nodes matching the given node pattern.
     */
    public Set<String> assertAllShardsOnNodes(final String index, final String... pattern) {
        final Set<String> nodes = new HashSet<>();
        final ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        for (final IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (final IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (final ShardRouting shardRouting : indexShardRoutingTable) {
                    if (shardRouting.currentNodeId() != null && index.equals(shardRouting.getIndex())) {
                        final String name = clusterState.nodes().get(shardRouting.currentNodeId()).name();
                        nodes.add(name);
                        assertThat("Allocated on new node: " + name, Regex.simpleMatch(pattern, name), is(true));
                    }
                }
            }
        }
        return nodes;
    }

    /**
     * Asserts that there are no files in the specified path
     */
    public void assertPathHasBeenCleared(final String path) throws Exception {
        assertPathHasBeenCleared(Paths.get(path));
    }

    /**
     * Asserts that there are no files in the specified path
     */
    public void assertPathHasBeenCleared(final Path path) throws Exception {
        logger.info("--> checking that [{}] has been cleared", path);
        int count = 0;
        final StringBuilder sb = new StringBuilder();
        sb.append("[");
        if (Files.exists(path)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
                for (final Path file : stream) {
                    logger.info("--> found file: [{}]", file.toAbsolutePath().toString());
                    if (Files.isDirectory(file)) {
                        assertPathHasBeenCleared(file);
                    } else if (Files.isRegularFile(file)) {
                        count++;
                        sb.append(file.toAbsolutePath().toString());
                        sb.append("\n");
                    }
                }
            }
        }
        sb.append("]");
        assertThat(count + " files exist that should have been cleaned:\n" + sb.toString(), count, equalTo(0));
    }

    protected static class NumShards {
        public final int numPrimaries;
        public final int numReplicas;
        public final int totalNumShards;
        public final int dataCopies;

        private NumShards(final int numPrimaries, final int numReplicas) {
            this.numPrimaries = numPrimaries;
            this.numReplicas = numReplicas;
            this.dataCopies = numReplicas + 1;
            this.totalNumShards = numPrimaries * dataCopies;
        }
    }

    private static boolean runTestScopeLifecycle() {
        return INSTANCE == null;
    }

    @Before
    public final void before() throws Exception {
        if (runTestScopeLifecycle()) {
            beforeInternal();
        }
    }

    @After
    public final void after() throws Exception {
        if (runTestScopeLifecycle()) {
            afterInternal(false);
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (!runTestScopeLifecycle()) {
            try {
                INSTANCE.afterInternal(true);
            } finally {
                INSTANCE = null;
            }
        } else {
            clearClusters();
        }
        SUITE_SEED = null;
    }

    private static void initializeSuiteScope() throws Exception {
        final Class<?> targetClass = getContext().getTargetClass();
        /**
         * Note we create these test class instance via reflection
         * since JUnit creates a new instance per test and that is also
         * the reason why INSTANCE is static since this entire method
         * must be executed in a static context.
         */
        assert INSTANCE == null;
        if (isSuiteScopedTest(targetClass)) {
            // note we need to do this this way to make sure this is reproducible
            INSTANCE = (ElasticsearchIntegrationTest) targetClass.newInstance();
            boolean success = false;
            try {
                INSTANCE.beforeInternal();
                INSTANCE.setupSuiteScopeCluster();
                success = true;
            } finally {
                if (!success) {
                    afterClass();
                }
            }
        } else {
            INSTANCE = null;
        }
    }

    /**
     * Return settings that could be used to start a node that has the given zipped home directory.
     */
    protected Settings prepareBackwardsDataDir(final File backwardsIndex, final Object... settings) throws IOException {
        final File indexDir = newTempDir();
        final File dataDir = new File(indexDir, "data");
        TestUtil.unzip(backwardsIndex, indexDir);
        assertTrue(dataDir.exists());
        final String[] list = dataDir.list();
        if (list == null || list.length > 1) {
            throw new IllegalStateException("Backwards index must contain exactly one cluster");
        }
        final File src = new File(dataDir, list[0]);
        final File dest = new File(dataDir, internalCluster().getClusterName());
        assertTrue(src.exists());
        src.renameTo(dest);
        assertFalse(src.exists());
        assertTrue(dest.exists());
        final ImmutableSettings.Builder builder = ImmutableSettings.builder().put(settings).put("gateway.type", "local") // this is important we need to recover from gateway
                .put("path.data", dataDir.getPath());

        final File configDir = new File(indexDir, "config");
        if (configDir.exists()) {
            builder.put("path.conf", configDir.getPath());
        }
        return builder.build();
    }

    protected HttpRequestBuilder httpClient() {
        final NodesInfoResponse nodeInfos = client().admin().cluster().prepareNodesInfo().get();
        final NodeInfo[] nodes = nodeInfos.getNodes();
        assertTrue(nodes.length > 0);
        final TransportAddress publishAddress = randomFrom(nodes).getHttp().address().publishAddress();
        assertEquals(1, publishAddress.uniqueAddressTypeId());
        final InetSocketAddress address = ((InetSocketTransportAddress) publishAddress).address();
        return new HttpRequestBuilder(HttpClients.createDefault()).host(address.getHostName()).port(address.getPort());
    }

    /**
     * This method is executed iff the test is annotated with {@link SuiteScopeTest} before the first test of this class is executed.
     *
     * @see SuiteScopeTest
     */
    protected void setupSuiteScopeCluster() throws Exception {
    }

    private static boolean isSuiteScopedTest(final Class<?> clazz) {
        return clazz.getAnnotation(SuiteScopeTest.class) != null;
    }

    /**
     * If a test is annotated with {@link org.elasticsearch.test.ElasticsearchIntegrationTest.SuiteScopeTest} the checks and modifications
     * that are applied to the used test cluster are only done after all tests
     * of this class are executed. This also has the side-effect of a suite level setup method {@link #setupSuiteScopeCluster()} that is
     * executed in a separate test instance. Variables that need to be accessible across test instances must be static.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Inherited
    @Ignore
    public @interface SuiteScopeTest {
    }
}