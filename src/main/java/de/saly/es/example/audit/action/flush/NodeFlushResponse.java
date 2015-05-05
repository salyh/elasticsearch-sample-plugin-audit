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

import org.elasticsearch.action.support.nodes.NodeOperationResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class NodeFlushResponse extends NodeOperationResponse {

    private int count;

    NodeFlushResponse() {
    }

    NodeFlushResponse(final DiscoveryNode node, final int count) {
        super(node);
        this.count = count;
    }

    public int getCount() {
        return count;
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
