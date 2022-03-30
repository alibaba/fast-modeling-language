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

import java.util.Set;
import java.util.regex.Pattern;

import com.aliyun.fastmodel.driver.cli.command.Console;
import com.aliyun.fastmodel.parser.StatementSplitter;
import com.aliyun.fastmodel.parser.generate.FastModelLexer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenSource;
import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Locale.ENGLISH;

/**
 * input high lighter
 *
 * @author panguanjing
 * @date 2021/1/26
 */
public class InputHighlighter implements Highlighter {

    private static final AttributedStyle KEYWORD_STYLE = AttributedStyle.BOLD;

    private static final AttributedStyle STRING_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN);

    private static final AttributedStyle NUMBER_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.CYAN);

    private static final AttributedStyle COMMENT_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.BRIGHT)
        .italic();

    private static final AttributedStyle ERROR_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.RED);

    private static final Set<String> KEYWORDS = StatementSplitter.keywords().stream()
        .map(keyword -> keyword.toLowerCase(ENGLISH))
        .collect(toImmutableSet());

    @Override
    public AttributedString highlight(LineReader reader, String buffer) {
        TokenSource tokenSource = StatementSplitter.getLexer(buffer, Console.STATEMENT_DELIMITERS);
        AttributedStringBuilder builder = new AttributedStringBuilder();
        boolean error = false;
        while (true) {
            Token token = tokenSource.nextToken();
            int type = token.getType();
            if (type == Token.EOF) {
                break;
            }
            String text = token.getText();
            if (error || (type == FastModelLexer.UNRECOGNIZED)) {
                error = true;
                builder.styled(ERROR_STYLE, text);
            } else if (isKeyWord(text)) {
                builder.styled(KEYWORD_STYLE, text);
            } else if (isString(type)) {
                builder.styled(STRING_STYLE, text);
            } else if (isNumber(type)) {
                builder.styled(NUMBER_STYLE, text);
            } else if (isComment(type)) {
                builder.styled(COMMENT_STYLE, text);
            } else {
                builder.append(text);
            }
        }
        return builder.toAttributedString();
    }

    private boolean isString(int type) {
        return type == FastModelLexer.StringLiteral;
    }

    private static boolean isKeyWord(String text) {
        return KEYWORDS.contains(text);
    }

    private static boolean isNumber(int type) {
        return (type == FastModelLexer.INTEGER_VALUE) ||
            (type == FastModelLexer.DOUBLE_VALUE) ||
            (type == FastModelLexer.DECIMAL_VALUE);
    }

    private static boolean isComment(int type) {
        return (type == FastModelLexer.SINGLE_LINE_COMMENT) ||
            (type == FastModelLexer.MULTILINE_COMMENT);
    }

    @Override
    public void setErrorPattern(Pattern errorPattern) {

    }

    @Override
    public void setErrorIndex(int errorIndex) {

    }

}
