/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.resource;

public final class ResourceUtils {

    private ResourceUtils() {}

    public static final String WILDCARD_MARKER = "*";

    public static boolean matchResource(Resource stored, Resource input) {
        return matchResource(stored, input.toFilter());
    }

    public static boolean matchResource(Resource stored, ResourceFilter input) { // TODO matching criteria should be different for delete call?
        if (!input.resourceType().equals(ResourceType.ANY) && !input.resourceType().equals(stored.resourceType())) {
            return false;
        }
        switch (stored.resourceNameType()) {
            case LITERAL:
                switch (input.resourceNameType()) {
                    case ANY:
                        return input.name() == null
                                || input.name().equals(stored.name())
                                || stored.name().equals(WILDCARD_MARKER);
                    case LITERAL:
                        return input.name() == null
                                || stored.name().equals(input.name())
                                || stored.name().equals(WILDCARD_MARKER);
                    case WILDCARD_SUFFIXED:
                        return false;
                    default:
                        return false;
                }
            case WILDCARD_SUFFIXED:
                switch (input.resourceNameType()) {
                    case ANY:
                        return input.name() == null
                                || matchWildcardSuffixedString(stored.name() + WILDCARD_MARKER, input.name())
                                || stored.name().equals(input.name());
                    case LITERAL:
                        return input.name() == null
                                || matchWildcardSuffixedString(stored.name() + WILDCARD_MARKER, input.name());
                    case WILDCARD_SUFFIXED:
                        return stored.name().equals(input.name());
                    default:
                        return false;
                }
            default:
                return false;
        }
    }

    /**
     * Returns true if two strings match, both of which might end with a WILDCARD_MARKER (which matches everything).
     * Examples:
     *   matchWildcardSuffixedString("rob", "rob") => true
     *   matchWildcardSuffixedString("*", "rob") => true
     *   matchWildcardSuffixedString("ro*", "rob") => true
     *   matchWildcardSuffixedString("rob", "bob") => false
     *   matchWildcardSuffixedString("ro*", "bob") => false
     *
     *   matchWildcardSuffixedString("rob", "*") => true
     *   matchWildcardSuffixedString("rob", "ro*") => true
     *   matchWildcardSuffixedString("bob", "ro*") => false
     *
     *   matchWildcardSuffixedString("ro*", "ro*") => true
     *   matchWildcardSuffixedString("rob*", "ro*") => false
     *   matchWildcardSuffixedString("ro*", "rob*") => true
     *
     * @param wildcardSuffixedPattern Value stored in ZK in either resource name or Acl.
     * @param resourceName Value present in the request.
     * @return true if there is a match (including wildcard-suffix matching).
     */
    static boolean matchWildcardSuffixedString(String wildcardSuffixedPattern, String resourceName) { // TODO review this method again after design changes

        if (wildcardSuffixedPattern.equals(resourceName) || wildcardSuffixedPattern.equals(WILDCARD_MARKER) || resourceName.equals(WILDCARD_MARKER)) {
            // if strings are equal or either of acl or resourceName is a wildcard
            return true;
        }

        if (wildcardSuffixedPattern.endsWith(WILDCARD_MARKER)) {

            String aclPrefix = wildcardSuffixedPattern.substring(0, wildcardSuffixedPattern.length() - WILDCARD_MARKER.length());

            if (resourceName.endsWith(WILDCARD_MARKER)) {
                // when both acl and resourceName ends with wildcard, non-wildcard prefix of resourceName should start with non-wildcard prefix of acl
                String inputPrefix = resourceName.substring(0, resourceName.length() - WILDCARD_MARKER.length());
                return inputPrefix.startsWith(aclPrefix);
            }

            // when acl ends with wildcard but resourceName doesn't, then resourceName should start with non-wildcard prefix of acl
            return resourceName.startsWith(aclPrefix);

        } else {

            if (resourceName.endsWith(WILDCARD_MARKER)) {
                // when resourceName ends with wildcard but acl doesn't, then acl should start with non-wildcard prefix of resourceName
                String inputPrefix = resourceName.substring(0, resourceName.length() - WILDCARD_MARKER.length());
                return wildcardSuffixedPattern.startsWith(inputPrefix);
            }

            // when neither acl nor resourceName ends with wildcard, they have to match exactly.
            return wildcardSuffixedPattern.equals(resourceName);

        }
    }
}
