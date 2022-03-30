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

package com.aliyun.fastmodel.parser;

import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser;
import com.aliyun.fastmodel.parser.lexer.DelimiterLexer;
import com.google.common.collect.ImmutableSet;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/12/28
 */
public class DelimiterLexerTest {

    @Test
    public void testLexer() {
        String sql = "show tables;";
        DelimiterLexer delimiterLexer = new DelimiterLexer(CharStreams.fromString(sql), ImmutableSet.of(";"));
        StringBuilder sb = new StringBuilder();
        while (true) {
            Token token = delimiterLexer.nextToken();
            if (token.getType() == Token.EOF) {
                break;
            }
            if (token.getType() == FastModelGrammarParser.DELIMITER) {
                assertEquals(sb.toString(), "show tables");
                break;
            } else {
                sb.append(token.getText());
            }
        }
    }
}