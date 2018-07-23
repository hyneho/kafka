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
package org.apache.kafka.common.security.oauthbearer.internals;

import org.apache.kafka.common.security.auth.SaslExtensions;
import org.apache.kafka.common.utils.Utils;

import javax.security.sasl.SaslException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OAuthBearerClientInitialResponse {
    static final String SEPARATOR = "\u0001";

    private static final String SASLNAME = "(?:[\\x01-\\x7F&&[^=,]]|=2C|=3D)+";
    private static final String KEY = "[A-Za-z]+";
    private static final String VALUE = "[\\x21-\\x7E \t\r\n]+";

    private static final String KVPAIRS = String.format("(%s=%s%s)*", KEY, VALUE, SEPARATOR);
    private static final Pattern AUTH_PATTERN = Pattern.compile("(?<scheme>[\\w]+)[ ]+(?<token>[-_\\.a-zA-Z0-9]+)");
    private static final Pattern CLIENT_INITIAL_RESPONSE_PATTERN = Pattern.compile(
            String.format("n,(a=(?<authzid>%s))?,%s(?<kvpairs>%s)%s", SASLNAME, SEPARATOR, KVPAIRS, SEPARATOR));
    private static final String AUTH_KEY = "auth";

    private final String tokenValue;
    private final String authorizationId;
    private SaslExtensions saslExtensions;

    public static final Pattern EXTENSION_KEY_PATTERN = Pattern.compile(KEY);
    public static final Pattern EXTENSION_VALUE_PATTERN = Pattern.compile(VALUE);

    public OAuthBearerClientInitialResponse(byte[] response) throws SaslException {
        String responseMsg = new String(response, StandardCharsets.UTF_8);
        Matcher matcher = CLIENT_INITIAL_RESPONSE_PATTERN.matcher(responseMsg);
        if (!matcher.matches())
            throw new SaslException("Invalid OAUTHBEARER client first message");
        String authzid = matcher.group("authzid");
        this.authorizationId = authzid == null ? "" : authzid;
        String kvPairs = matcher.group("kvpairs");
        Map<String, String> properties = Utils.parseMap(kvPairs, "=", SEPARATOR);
        String auth = properties.get(AUTH_KEY);
        if (auth == null)
            throw new SaslException("Invalid OAUTHBEARER client first message: 'auth' not specified");
        properties.remove(AUTH_KEY);
        extensions(new SaslExtensions(properties));

        Matcher authMatcher = AUTH_PATTERN.matcher(auth);
        if (!authMatcher.matches())
            throw new SaslException("Invalid OAUTHBEARER client first message: invalid 'auth' format");
        if (!"bearer".equalsIgnoreCase(authMatcher.group("scheme"))) {
            String msg = String.format("Invalid scheme in OAUTHBEARER client first message: %s",
                    matcher.group("scheme"));
            throw new SaslException(msg);
        }
        this.tokenValue = authMatcher.group("token");
    }

    public OAuthBearerClientInitialResponse(String tokenValue, SaslExtensions extensions) throws SaslException {
        this(tokenValue, "", extensions);
    }

    public OAuthBearerClientInitialResponse(String tokenValue, String authorizationId, SaslExtensions extensions) throws SaslException {
        this.tokenValue = tokenValue;
        this.authorizationId = authorizationId == null ? "" : authorizationId;
        extensions(extensions);
    }

    public SaslExtensions extensions() {
        return saslExtensions;
    }

    public byte[] toBytes() {
        String authzid = authorizationId.isEmpty() ? "" : "a=" + authorizationId;
        String extensions = extensionsMessage();
        if (extensions.length() > 0)
            extensions = SEPARATOR + extensions;

        String message = String.format("n,%s,%sauth=Bearer %s%s%s%s", authzid,
                SEPARATOR, tokenValue, extensions, SEPARATOR, SEPARATOR);

        return message.getBytes(StandardCharsets.UTF_8);
    }

    public String tokenValue() {
        return tokenValue;
    }

    public String authorizationId() {
        return authorizationId;
    }

    public String propertyValue(String name) {
        if (AUTH_KEY.equals(name))
            return tokenValue;
        return saslExtensions.extensionValue(name);
    }

    /**
     * Validates and sets the extensions
     */
    private void extensions(SaslExtensions extensions) throws SaslException {
        validateExtensions(extensions);
        saslExtensions = extensions;
    }

    /**
     * Validates that the given extensions conform to the standard
     *
     * @see <a href="https://tools.ietf.org/html/rfc7628#section-3.1">RFC 7628,
     *  Section 3.1</a>
     */
    private void validateExtensions(SaslExtensions extensions) throws SaslException {
        for (Map.Entry<String, String> entry : extensions.map().entrySet()) {
            String extensionName = entry.getKey();
            String extensionValue = entry.getValue();

            if (!EXTENSION_KEY_PATTERN.matcher(extensionName).matches())
                throw new SaslException("Extension name " + extensionName + " is invalid");
            if (!EXTENSION_VALUE_PATTERN.matcher(extensionValue).matches())
                throw new SaslException("Extension value (" + extensionValue + ") for extension " + extensionName + " is invalid");
        }
    }

    /**
     * Converts the SASLExtensions to an OAuth protocol-friendly string
     */
    private String extensionsMessage() {
        return Utils.mkString(saslExtensions.map(), "", "", "=", SEPARATOR);
    }
}
