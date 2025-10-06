/*
 *
 *  Copyright 2025 Datadobi
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software

 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.datadobi.s3test.s3;

import com.datadobi.s3test.util.SystemPropertyOption;

class S3ClientOption {
    public static final String OPTION_PREFIX = "com.datadobi.s3.sdk";

    public static final SystemPropertyOption<Integer> MAX_CONNECTIONS = SystemPropertyOption.createIntOption(OPTION_PREFIX, "max_connections");
    public static final SystemPropertyOption<Integer> CONNECT_TIMEOUT_SECONDS = SystemPropertyOption.createIntOption(OPTION_PREFIX, "connect_timout_seconds");
    public static final SystemPropertyOption<Integer> SOCKET_TIMEOUT_SECONDS = SystemPropertyOption.createIntOption(OPTION_PREFIX, "socket_timeout_seconds");

    public static final SystemPropertyOption<Integer> NUM_RETRIES = SystemPropertyOption.createIntOption(OPTION_PREFIX, "num_retries");
    public static final SystemPropertyOption<Integer> API_CALL_TIMEOUT_SECONDS = SystemPropertyOption.createIntOption(OPTION_PREFIX,
            "api_call_timeout_seconds");
    public static final SystemPropertyOption<Integer> API_CALL_ATTEMPT_TIMEOUT_SECONDS = SystemPropertyOption.createIntOption(OPTION_PREFIX,
            "api_call_attempt_timeout_seconds");
}