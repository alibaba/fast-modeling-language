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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser;
import com.aliyun.fastmodel.parser.generate.FastModelLexer;
import com.aliyun.fastmodel.parser.lexer.DelimiterLexer;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.Vocabulary;

/**
 * 语句的切割器
 *
 * @author panguanjing
 * @date 2020/12/28
 */
@Getter
public class StatementSplitter {

    private final List<StatementText> statements;

    private final String partialStatement;

    private static final Pattern IDENTIFIER = Pattern.compile("'([A-Z_]+)'");

    public StatementSplitter(String sql) {
        this(sql, ImmutableSet.of(";"));
    }

    public StatementSplitter(String sql, Set<String> delimiters) {
        TokenSource tokenSource = new DelimiterLexer(CharStreams.fromString(sql), delimiters);
        List<StatementText> statements = Lists.newArrayList();
        StringBuilder sb = new StringBuilder();
        while (true) {
            Token token = tokenSource.nextToken();
            if (token.getType() == Token.EOF) {
                break;
            }
            if (token.getType() == FastModelGrammarParser.DELIMITER) {
                String statement = sb.toString().trim();
                if (!statement.isEmpty()) {
                    statements.add(new StatementText(statement, token.getText()));
                }
                sb = new StringBuilder();
            } else {
                sb.append(token.getText());
            }
        }
        this.statements = statements;
        partialStatement = sb.toString().trim();
    }

    public static TokenSource getLexer(String buffer, Set<String> statementDelimiters) {
        return new DelimiterLexer(CharStreams.fromString(buffer), statementDelimiters);
    }

    public static boolean isEmptyStatement(String sql) {
        TokenSource tokens = getLexer(sql, ImmutableSet.of());
        while (true) {
            Token token = tokens.nextToken();
            if (token.getType() == Token.EOF) {
                return true;
            }
            if (token.getChannel() != Token.HIDDEN_CHANNEL) {
                return false;
            }
        }
    }

    public static Set<String> keywords() {
        Vocabulary vocabulary = FastModelLexer.VOCABULARY;
        Set<String> sets = new HashSet<>();
        for (int i = 0; i <= vocabulary.getMaxTokenType(); i++) {
            String name = Strings.nullToEmpty(vocabulary.getLiteralName(i));
            Matcher matcher = IDENTIFIER.matcher(name);
            if (matcher.matches()) {
                sets.add(matcher.group(1));
            }
        }
        return sets;
    }

    /**
     * 获取items
     *
     * @return
     */
    public static Set<Item> getVocabularyItems() {
        Vocabulary vocabulary = FastModelLexer.VOCABULARY;
        Set<Item> set = new HashSet<>(128);
        for (int i = 0; i <= vocabulary.getMaxTokenType(); i++) {
            String name = Strings.nullToEmpty(vocabulary.getLiteralName(i));
            Matcher matcher = IDENTIFIER.matcher(name);
            if (matcher.matches()) {
                Item item = new Item();
                String group = matcher.group(1);
                String symbolicName = vocabulary.getSymbolicName(i);
                item.setKeyword(group);
                item.setSymbolName(symbolicName);
                set.add(item);
            }
        }
        return set;
    }

    @Getter
    @Setter
    @EqualsAndHashCode
    public static class Item {
        private String symbolName;
        private String keyword;
    }

    @Getter
    @EqualsAndHashCode(callSuper = false)
    public static class StatementText {
        private final String statement;
        private final String terminator;

        public StatementText(String statement, String terminator) {
            this.statement = statement;
            this.terminator = terminator;
        }
    }

}
