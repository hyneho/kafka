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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.InvalidConfigurationException;

import java.util.Properties;
import java.io.InputStream;
import java.io.FileInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetchResponseParserFactory {
    public static final Logger log = LoggerFactory.getLogger(FetchResponseParserFactory.class);

    public static final String FETCH_RESPONSE_PARSER_PROPERTY = "org.apache.kafka.common.requests.FetchResponseParser";
    public static final String FETCH_RESPONSE_PARSER_ENV = "KAFKA_FETCH_RESPONSE_PARSER";
    public static final String FETCH_RESPONSE_PARSER_DEFAULT = "org.apache.kafka.common.requests.DefaultFetchResponseParser";

    private static String getFetchResponseParserClassName() {
        String fetchResponseParserClassName = System.getProperty(FETCH_RESPONSE_PARSER_PROPERTY);
        if (null != fetchResponseParserClassName) {
            log.info("FetchResponseParser class {} from property {}", fetchResponseParserClassName, FETCH_RESPONSE_PARSER_PROPERTY);
            return fetchResponseParserClassName;
        }

        fetchResponseParserClassName = System.getenv(FETCH_RESPONSE_PARSER_ENV);
        if (null != fetchResponseParserClassName) {
            log.info("FetchResponseParser class {} from env {}", fetchResponseParserClassName, FETCH_RESPONSE_PARSER_ENV);
            return fetchResponseParserClassName;
        }

        fetchResponseParserClassName = getFetchResponseParserClassNameFromConfigFile();
        if (null != fetchResponseParserClassName) {
            return fetchResponseParserClassName;
        }

        fetchResponseParserClassName = FETCH_RESPONSE_PARSER_DEFAULT;
        log.info("FetchResponseParser class {} from default {}", fetchResponseParserClassName, FETCH_RESPONSE_PARSER_DEFAULT);
        return fetchResponseParserClassName;
    }

    private static String getFetchResponseParserClassNameFromConfigFile() {
        String commandLine = System.getProperty("sun.java.command");
        if(null == commandLine) {
            return null;
        }

        String[] commandLineArgs = commandLine.split("\\s+");
        String configFileName = null;
        if(commandLineArgs.length < 2) {
            return null;
        }

        configFileName = commandLineArgs[1];
        if(null == configFileName) {
            return null;
        }

        Properties properties = new Properties();
        try {
            InputStream inputStream = new FileInputStream(configFileName);
            properties.load(inputStream);
            inputStream.close();
            inputStream = null;
        } catch(Exception e) {
            log.trace("Failed to load {}", configFileName, e);
            return null;
        }

        String fetchResponseParserClassName = null;
        try {
            fetchResponseParserClassName = properties.getProperty(FETCH_RESPONSE_PARSER_PROPERTY);
        } catch(Exception e) {
            log.trace("{} not found in {}", FETCH_RESPONSE_PARSER_PROPERTY, configFileName, e);
            return null;
        }

        if(null == fetchResponseParserClassName) {
            return null;
        }

        log.info("FetchResponseParser class {} from config {}", fetchResponseParserClassName, configFileName);
        return fetchResponseParserClassName;
    }


    public static FetchResponseParser getFetchResponseParser() {
        try {
            String fetchResponseParserClassName = getFetchResponseParserClassName();
            return (FetchResponseParser) Class.forName(fetchResponseParserClassName).getConstructor().newInstance();
        } catch (Exception e) {
            String message = "Failed to initialize";
            log.error(message, e);
            throw new InvalidConfigurationException(message, e);
        }
    }
}

