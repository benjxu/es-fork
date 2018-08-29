/*
 * Copyright [2017] h.xu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.gosololaw.elasticsearch;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.elasticsearch.plugins.Plugin;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.util.Base64;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.hamcrest.CoreMatchers.is;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

@ClusterScope(scope = Scope.SUITE)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@RunWith(com.carrotsearch.randomizedtesting.RandomizedRunner.class)
public class EsIntegrationTests extends ESIntegTestCase {
    private Client client;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.client = client();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(VectorScoringPlugin.class);
    }

    @Test
    public void testSearchVector() throws Exception {
        createIndex("test");

        // GIVEN
        indexVectors("1");
        indexVectors("2");
        indexVectors("3");
        refresh();

        // WHEN
        SearchResponse searchResponse = this.client.prepareSearch("test", "test")
//                .setQuery(QueryBuilders.wrapperQuery(getQuery()))
                .execute().actionGet();;

        // THEN
        assertThat(searchResponse.getHits().totalHits, is(3L));
    }

    private void indexVectors(String id) throws Exception {
        this.client.prepareIndex("test", "test", id)
                .setSource("content_vector", convertArrayToBase64(new double[]{0.2,0.3,0.1,0.5})).execute().actionGet();

    }


    private static String convertArrayToBase64(double[] array) {
        final int capacity = 8 * array.length;
        final ByteBuffer bb = ByteBuffer.allocate(capacity);
        for (int i = 0; i < array.length; i++) {
            bb.putDouble(array[i]);
        }
        bb.rewind();
        final ByteBuffer encodedBB = Base64.getEncoder().encode(bb);
        return new String(encodedBB.array());
    }

    private static double[] convertBase64ToArray(String base64Str) {
        final byte[] decode = Base64.getDecoder().decode(base64Str.getBytes());
        final DoubleBuffer doubleBuffer = ByteBuffer.wrap(decode).asDoubleBuffer();

        final double[] dims = new double[doubleBuffer.capacity()];
        doubleBuffer.get(dims);
        return dims;
    }

    private String getQuery() throws Exception {
        String json = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("query")
                .startObject("function_score")
                .startObject("query")
                .startObject("match")
                .field("name", "Ben")
                .endObject()
                .endObject()
                .startArray("functions")
                .startObject()
                .startObject("script_score")
                .startObject("script")
                .field("source", "vector_scoring")
                .field("lang", "binary_vector_score")
                .startObject("params")
                .field("vector_field", "content_vector")
                .field("vector", new double[]{0.1, 0.2, 0.3, 0.4})
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .toString();
        return json;
    }
}