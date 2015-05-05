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

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import de.saly.es.example.audit.action.flush.FlushAction;
import de.saly.es.example.audit.action.flush.FlushRequest;
import de.saly.es.example.audit.action.flush.FlushResponse;

@ThreadLeakScope(com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope.NONE)
public class AuditTest extends AbstractUnitTest {

    @Test
    public void testSecureRandomScript1() throws Exception {

        // Create a new index
        final String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("name").field("type", "string").endObject().startObject("number").field("type", "integer").endObject()
                .endObject().endObject().endObject().string();

        assertAcked(prepareCreate("test").addMapping("type", mapping));

        assertAcked(prepareCreate("test1").addMapping("type", mapping));

        index("test", "type", "1doc", "{\"a\":1}");
        index("test", "type", "2doc", "{\"a\":2}");
        index("test", "type", "3doc", "{\"a\":3}");
        index("test", "type", "4doc", "{\"a\":4}");
        index("test", "type", "5doc", "{\"a\":5,\"b\":3}");

        for (int i = 0; i < 30; i++) {
            client().prepareIndex("test", "type").setSource("{\"dummy\":0}").get();
        }

        final int bulkCount = 4000;

        final List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        for (int i = 0; i < bulkCount; i++) {
            indexBuilders.add(client().prepareIndex("test", "type", Integer.toString(i)).setSource(
                    XContentFactory.jsonBuilder().startObject().field("name", "rec " + i).field("number", i).endObject()));
        }

        indexRandom(true, false, indexBuilders);

        client().prepareUpdate("test", "type", "5doc").setScript("ctx._source.b++", ScriptType.INLINE).get();
        client().prepareIndex("test", "type", "4doc").setSource("{\"a\":44}").get();
        client().prepareDelete("test", "type", "4doc").get();
        client().prepareDeleteByQuery("test", "test1").setQuery(new IdsQueryBuilder("type").addIds("1doc", "2doc")).get();

        refresh();

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchAllQuery()).execute().actionGet();

        assertNoFailures(searchResponse);
        assertHitCountWithMsg("testsearch", searchResponse, 32 + bulkCount);

        int flushCount = 0;
        do {
            final FlushResponse res = client().admin().cluster().execute(FlushAction.INSTANCE, new FlushRequest()).actionGet();
            flushCount = res.getCount();
            refresh();
            logger.info("Flushed {} msgs", flushCount);
        } while (flushCount > 0);

        searchResponse = client().prepareSearch("audit").setQuery(matchAllQuery()).execute().actionGet();

        assertNoFailures(searchResponse);
        System.out.println("Audit search result " + searchResponse.getHits().totalHits());
        //assertHitCountWithMsg("auditsearch",searchResponse, flushCount+bulkCount);
    }
}
