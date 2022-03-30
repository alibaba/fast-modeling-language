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

package com.aliyun.fastmodel.core.parser;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import com.aliyun.fastmodel.core.exception.ParserNotFoundException;

/**
 * 工厂类的方式，用动态方法进行加载处理:
 *
 * @author panguanjing
 * @date 2020/10/15
 */
public class FastModelParserFactory {

    private final static FastModelParserFactory INSTANCE = new FastModelParserFactory();

    private final Map<String, LanguageParser> map = new HashMap<>(4);

    private FastModelParserFactory() {
        loadParser(LanguageParser.class);
    }

    private void loadParser(Class<? extends LanguageParser> clazz) {
        ServiceLoader<? extends LanguageParser> loader = ServiceLoader.load(clazz);
        for (LanguageParser fastModelParser : loader) {
            map.put(fastModelParser.getClass().getName(), fastModelParser);
        }
    }

    public static FastModelParserFactory getInstance() {
        return INSTANCE;
    }

    /**
     * 加载默认的parser
     *
     * @return {@link FastModelParser}
     * @throws ParserNotFoundException 没有找到Parser
     */
    public FastModelParser get() throws ParserNotFoundException {
        FastModelParser fastModelParser = (FastModelParser)map.values().stream().filter(parser -> {
            return parser instanceof FastModelParser;
        }).findFirst().orElse(null);
        if (fastModelParser == null) {
            throw new ParserNotFoundException("load default FastModelParser not Found");
        }
        return fastModelParser;
    }
}
