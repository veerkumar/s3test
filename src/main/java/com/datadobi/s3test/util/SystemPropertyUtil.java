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
package com.datadobi.s3test.util;

import javax.annotation.Nullable;
import java.util.function.Function;

public class SystemPropertyUtil {
    @Nullable
    public static <T> T getValue(String name, @Nullable T defaultValue, Function<String, T> parse) {
        String property = System.getProperty(name);
        if (property == null) {
            return defaultValue;
        }

        try {
            T parsed = parse.apply(property);
            if (parsed != null) {
                return parsed;
            } else {
                return defaultValue;
            }
        } catch (RuntimeException e) {
            return defaultValue;
        }
    }

    public static boolean getBoolean(String name, boolean defaultValue) {
        return getValue(name, defaultValue, Boolean::valueOf);
    }

    @Nullable
    public static Boolean getBoolean(String name) {
        return getValue(name, null, Boolean::valueOf);
    }

    public static int getInt(String name, int defaultValue) {
        return getValue(name, defaultValue, Integer::parseInt);
    }

    @Nullable
    public static Integer getInt(String name) {
        return getValue(name, null, Integer::parseInt);
    }

    public static long getLong(String name, long defaultValue) {
        return getValue(name, defaultValue, Long::parseLong);
    }

    @Nullable
    public static Long getLong(String name) {
        return getValue(name, null, Long::parseLong);
    }

    @Nullable
    public static String getString(String name, @Nullable String defaultValue) {
        String property = System.getProperty(name);
        if (property == null) {
            return defaultValue;
        }

        return property;
    }
}
