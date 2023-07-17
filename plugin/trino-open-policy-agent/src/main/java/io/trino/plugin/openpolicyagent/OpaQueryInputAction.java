/*
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
 */
package io.trino.plugin.openpolicyagent;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import io.trino.spi.security.TrinoPrincipal;

import java.util.Collection;
import java.util.List;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class OpaQueryInputAction
{
    public final String operation;
    public final OpaQueryInputResource resource;
    public final List<OpaQueryInputResource> filterResources;
    public final OpaQueryInputResource targetResource;
    public final OpaQueryInputGrant grantee;
    public final TrinoPrincipal grantor;

    private OpaQueryInputAction(OpaQueryInputAction.Builder builder)
    {
        this.operation = builder.operation;
        this.resource = builder.resource;
        this.filterResources = builder.filterResources;
        this.targetResource = builder.targetResource;
        this.grantee = builder.grantee;
        this.grantor = builder.grantor;
        if (this.resource != null && this.filterResources != null) {
            throw new IllegalArgumentException("resource and filterResources cannot both be configured");
        }
    }

    public static class Builder
    {
        private String operation;
        private OpaQueryInputResource resource;
        private List<OpaQueryInputResource> filterResources;
        private OpaQueryInputResource targetResource;
        private OpaQueryInputGrant grantee;
        private TrinoPrincipal grantor;

        public Builder operation(String operation)
        {
            this.operation = operation;
            return this;
        }

        public Builder resource(OpaQueryInputResource resource)
        {
            this.resource = resource;
            return this;
        }

        public Builder filterResources(Collection<OpaQueryInputResource> resources)
        {
            this.filterResources = List.copyOf(resources);
            return this;
        }

        public Builder targetResource(OpaQueryInputResource targetResource)
        {
            this.targetResource = targetResource;
            return this;
        }

        public Builder grantee(OpaQueryInputGrant grantee)
        {
            this.grantee = grantee;
            return this;
        }

        public Builder grantor(TrinoPrincipal grantor)
        {
            this.grantor = grantor;
            return this;
        }

        public OpaQueryInputAction build()
        {
            return new OpaQueryInputAction(this);
        }
    }
}
