/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.utils;

import org.apache.paimon.CoreOptions;

import java.util.HashMap;
import java.util.Map;

/** Utils for procedure. */
public class ProcedureUtils {

    public static Map<String, String> fillInPartitionOptions(
            String expireStrategy,
            String timestampFormatter,
            String timestampPattern,
            String expirationTime,
            Integer maxExpires,
            String options) {
        Map<String, String> dynamicOptions = new HashMap<>();
        if (!StringUtils.isNullOrWhitespaceOnly(expireStrategy)) {
            dynamicOptions.put(CoreOptions.PARTITION_EXPIRATION_STRATEGY.key(), expireStrategy);
        }
        if (!StringUtils.isNullOrWhitespaceOnly(timestampFormatter)) {
            dynamicOptions.put(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), timestampFormatter);
        }
        if (!StringUtils.isNullOrWhitespaceOnly(timestampPattern)) {
            dynamicOptions.put(CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(), timestampPattern);
        }
        if (!StringUtils.isNullOrWhitespaceOnly(expirationTime)) {
            dynamicOptions.put(CoreOptions.PARTITION_EXPIRATION_TIME.key(), expirationTime);
        }
        if (maxExpires != null) {
            dynamicOptions.put(
                    CoreOptions.PARTITION_EXPIRATION_MAX_NUM.key(), String.valueOf(maxExpires));
        }
        if (!StringUtils.isNullOrWhitespaceOnly(options)) {
            dynamicOptions.putAll(ParameterUtils.parseCommaSeparatedKeyValues(options));
        }
        return dynamicOptions;
    }
}
