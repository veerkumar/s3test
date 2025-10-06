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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.function.Function;

public class SystemPropertyOption<T> {
    private static final Logger LOG = LoggerFactory.getLogger(SystemPropertyOption.class);

    public static SystemPropertyOption<Boolean> createBooleanOption(String prefix, String name) {
        return createOption(prefix, name, SystemPropertyUtil::getBoolean);
    }

    public static SystemPropertyOption<Integer> createIntOption(String prefix, String name) {
        return createOption(prefix, name, SystemPropertyUtil::getInt);
    }

    public static SystemPropertyOption<Long> createLongOption(String prefix, String name) {
        return createOption(prefix, name, SystemPropertyUtil::getLong);
    }

    public static <T> SystemPropertyOption<T> createOption(String prefix, String name, Function<String, T> getValue) {
        return createOption(prefix, name, getValue, Object::toString);
    }

    public static <T> SystemPropertyOption<T> createOption(String prefix, String name, Function<String, T> getValue, Function<T, String> formatValue) {
        String optionName = prefix + "." + name.toLowerCase();
        T value = getValue.apply(optionName);
        LOG.info("{}: {}", optionName, value == null ? "not set" : formatValue.apply(value));
        return new SystemPropertyOption<>(value);
    }

    private final @Nullable T value;

    private SystemPropertyOption(@Nullable T value) {
        this.value = value;
    }

    @Nullable
    public T getValue() {
        return getValue(null);
    }

    @Nullable
    public T getValue(@Nullable T defaultValue) {
        return value == null ? defaultValue : value;
    }
}
