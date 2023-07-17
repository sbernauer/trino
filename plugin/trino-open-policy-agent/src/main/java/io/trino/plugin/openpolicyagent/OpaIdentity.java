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

import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;

import java.util.Map;
import java.util.Set;

public record OpaIdentity(
        String user,
        Set<String> groups,
        Set<String> enabledRoles,
        Map<String, SelectedRole> catalogRoles,
        Map<String, String> extraCredentials)
{
    public static OpaIdentity fromTrinoIdentity(Identity identity)
    {
        return new OpaIdentity(
                identity.getUser(),
                identity.getGroups(),
                identity.getEnabledRoles(),
                identity.getCatalogRoles(),
                identity.getExtraCredentials());
    }
}
