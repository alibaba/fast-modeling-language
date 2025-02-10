/*
 * Copyright [2024] [name of copyright owner]
 *
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

package com.aliyun.fastmodel.transform.flink.parser.util;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.fastmodel.transform.flink.parser.FlinkSqlParser;
import com.google.common.base.Strings;
import org.antlr.v4.runtime.Vocabulary;

/**
 * @author 子梁
 * @date 2024/9/11
 */
public class FlinkReservedWordUtil {

    private static final Pattern KW_IDENTIFIER = Pattern.compile("(KW_)([A-Z0-9_]+)");

    private static final Set<String> SET = new HashSet<>();

    static {
        Vocabulary vocabulary = FlinkSqlParser.VOCABULARY;
        for (int i = 0; i <= vocabulary.getMaxTokenType(); i++) {
            String name = Strings.nullToEmpty(vocabulary.getSymbolicName(i));
            Matcher matcher = KW_IDENTIFIER.matcher(name);
            if (matcher.matches()) {
                SET.add(matcher.group(2));
            }
        }
    }

    public static boolean isReservedKeyWord(String word) {
        return SET.contains(word.toUpperCase());
    }

}
