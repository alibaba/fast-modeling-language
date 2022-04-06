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

package com.aliyun.fastmodel.parser.lexer;

import java.util.Set;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.parser.StatementSplitter;
import org.apache.commons.lang3.StringUtils;

import static java.util.Locale.ENGLISH;

/**
 * 一个保留关键字的判断的类。
 *
 * @author panguanjing
 * @date 2021/2/22
 */
public class ReservedIdentifier {

    private ReservedIdentifier() {}

    private static final Set<String> KEYWORDS = StatementSplitter.keywords().stream()
        .map(keyword -> keyword.toLowerCase(ENGLISH))
        .collect(Collectors.toSet());

    /**
     * 判断一个文本单词是否为关键字
     *
     * @param text
     * @return
     */
    public static boolean isKeyWord(String text) {
        if (StringUtils.isBlank(text)) {
            return false;
        }
        return KEYWORDS.contains(text.toLowerCase(ENGLISH));
    }

    public static Set<String> getKeywords() {
        return KEYWORDS;
    }
}
