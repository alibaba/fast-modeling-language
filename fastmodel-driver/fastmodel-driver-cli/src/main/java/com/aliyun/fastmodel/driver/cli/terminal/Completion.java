/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.driver.cli.terminal;

import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import org.jline.reader.Completer;
import org.jline.reader.impl.completer.StringsCompleter;

/**
 * completion
 *
 * @author panguanjing
 * @date 2021/1/26
 */
public class Completion {

    private static final Set<String> COMMANDS = ImmutableSet.of(
        "SHOW LAYERS",
        "SHOW DOMAINS",
        "SHOW TABLES",
        "SHOW INDICATORS",
        "SHOW ADJUNCTS",
        "SHOW TIMEPERIODS",
        "CREATE DIM TABLE",
        "CREATE FACT TABLE",
        "CREATE DWS TABLE",
        "CREATE ADS TABLE",
        "HELP",
        "QUIT",
        "DESCRIBE"
    );

    private Completion() {

    }

    public static Completer commandCompleter() {
        return new StringsCompleter(ImmutableSet.<String>builder().addAll(
                COMMANDS
            ).addAll(COMMANDS.stream().map(value -> value.toLowerCase(Locale.ENGLISH)).collect(Collectors.toSet()))
            .build());
    }
}
