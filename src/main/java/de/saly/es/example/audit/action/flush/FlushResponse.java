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

package de.saly.es.example.audit.action.flush;

import java.io.IOException;

import org.elasticsearch.action.support.nodes.NodesOperationResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class FlushResponse extends NodesOperationResponse<NodeFlushResponse> implements ToXContent {

    private int count;

    public FlushResponse() {
    }

    public FlushResponse(final ClusterName clusterName, final NodeFlushResponse[] nodes) {
        super(clusterName, nodes);

        for (int i = 0; i < nodes.length; i++) {
            final NodeFlushResponse nodeFlushResponse = nodes[i];
            count += nodeFlushResponse.getCount();
        }
    }

    public FlushResponse(final int count) {
        super();
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.field("count", count);
        return builder;
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
        count = in.readInt();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(count);
    }

}
