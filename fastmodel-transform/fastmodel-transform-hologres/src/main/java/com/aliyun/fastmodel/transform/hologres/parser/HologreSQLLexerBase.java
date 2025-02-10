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

package com.aliyun.fastmodel.transform.hologres.parser;

import java.util.ArrayDeque;
import java.util.Deque;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Lexer;

/**
 * base template code
 * HologreSQLLexerBase
 *
 * @author xuanjia
 * @date 2024/09/04
 */
public abstract class HologreSQLLexerBase extends Lexer {
    protected final Deque<String> tags = new ArrayDeque<>();

    protected HologreSQLLexerBase(CharStream input) {
        super(input);

    }

    public void pushTag() {
        tags.push(getText());
    }

    public boolean isTag() {
        return getText().equals(tags.peek());
    }

    public void popTag() {
        tags.pop();
    }

    public boolean checkLA(int c) {
        return getInputStream().LA(1) != c;
    }

    public boolean charIsLetter() {
        return Character.isLetter(getInputStream().LA(-1));
    }

    public void HandleNumericFail() {
        getInputStream().seek(getInputStream().index() - 2);
        setType(HologreSQLLexer.Integral);
    }

    public void HandleLessLessGreaterGreater() {
        if (getText() == "<<") {setType(HologreSQLLexer.LESS_LESS);}
        if (getText() == ">>") {setType(HologreSQLLexer.GREATER_GREATER);}
    }

    public void UnterminatedBlockCommentDebugAssert() {
        //Debug.Assert(InputStream.LA(1) == -1 /*EOF*/);
    }

    public boolean CheckIfUtf32Letter() {
        int codePoint = getInputStream().LA(-2) << 8 + getInputStream().LA(-1);
        char[] c;
        if (codePoint < 0x10000) {
            c = new char[] {(char)codePoint};
        } else {
            codePoint -= 0x10000;
            c = new char[] {(char)(codePoint / 0x400 + 0xd800), (char)(codePoint % 0x400 + 0xdc00)};
        }
        return Character.isLetter(c[0]);
    }
}