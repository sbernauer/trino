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
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;

import java.util.Set;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class OpaQueryInputGrant
{
    public Set<TrinoPrincipal> principals;
    public Boolean grantOption;
    public Privilege privilege;

    private OpaQueryInputGrant(Builder builder)
    {
        this.principals = builder.principals;
        this.grantOption = builder.grantOption;
        this.privilege = builder.privilege;
    }

    public static class Builder
    {
        private Set<TrinoPrincipal> principals;
        private Boolean grantOption;
        private Privilege privilege;

        public Builder principal(TrinoPrincipal principal)
        {
            this.principals = Set.of(principal);
            return this;
        }

        public Builder principals(Set<TrinoPrincipal> principals)
        {
            this.principals = Set.copyOf(principals);
            return this;
        }

        public Builder grantOption(boolean grantOption)
        {
            this.grantOption = grantOption;
            return this;
        }

        public Builder privilege(Privilege privilege)
        {
            this.privilege = privilege;
            return this;
        }

        public OpaQueryInputGrant build()
        {
            return new OpaQueryInputGrant(this);
        }
    }
}
