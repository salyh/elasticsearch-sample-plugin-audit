/*
Copyright 2015 Hendrik Saly

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package de.saly.es.example.audit.plugin;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Assert;

@ClusterScope(scope = Scope.TEST, numDataNodes = 3, numClientNodes = 2)
public abstract class AbstractUnitTest extends ElasticsearchIntegrationTest {

    @Override
    public Settings indexSettings() {
        final ImmutableSettings.Builder builder = ImmutableSettings.builder();
        builder.put(SETTING_NUMBER_OF_SHARDS, 3);
        builder.put(SETTING_NUMBER_OF_REPLICAS, 1);
        return builder.build();
    }

    @Override
    protected Settings nodeSettings(final int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder().put(super.nodeSettings(nodeOrdinal)).put("gateway.type", "none")
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true).put("script.disable_dynamic", false)
                //.put("node.local", false)
                .put("http.cors.enabled", true).put("http.enabled", true).build();
    }

    @Override
    protected Settings transportClientSettings() {
        return ImmutableSettings.builder().put("plugins.load_classpath_plugins", true).build();
    }

    public static void assertHitCountWithMsg(final String msg, final SearchResponse searchResponse, final long expectedHitCount) {
        if (searchResponse.getHits().totalHits() != expectedHitCount) {
            Assert.fail("Hit count is " + searchResponse.getHits().totalHits() + " but " + expectedHitCount + " was expected. "
                    + ElasticsearchAssertions.formatShardStatus(searchResponse) + " due to " + msg);
        }

        ElasticsearchAssertions.assertVersionSerializable(searchResponse);
    }

}
