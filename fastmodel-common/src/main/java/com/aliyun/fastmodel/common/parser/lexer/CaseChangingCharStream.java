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

package com.aliyun.fastmodel.common.parser.lexer;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;

/**
 * https://github.com/antlr/antlr4/blob/master/doc/resources/CaseChangingCharStream.java
 * 因为antlr当在语法文件中，使用大小写的时候，生成的vocabulary里面是空的，所以使用caseChangeCharStream的另外一种方式进行处理。
 * 具体可以查看：
 * https://github.com/antlr/antlr4/blob/master/doc/case-insensitive-lexing.md
 *
 * @author panguanjing
 * @date 2021/2/23
 */
public class CaseChangingCharStream implements CharStream {

    final CharStream charStream;
    final boolean upper;

    public CaseChangingCharStream(CharStream charStream, boolean upper) {
        this.charStream = charStream;
        this.upper = upper;
    }

    @Override
    public String getText(Interval interval) {
        return charStream.getText(interval);
    }

    @Override
    public void consume() {
        charStream.consume();
    }

    @Override
    public int LA(int i) {
        int c = charStream.LA(i);
        if (c <= 0) {
            return c;
        }
        if (upper) {
            return Character.toUpperCase(c);
        }
        return Character.toLowerCase(c);
    }

    @Override
    public int mark() {
        return charStream.mark();
    }

    @Override
    public void release(int marker) {
        charStream.release(marker);
    }

    @Override
    public int index() {
        return charStream.index();
    }

    @Override
    public void seek(int index) {
        charStream.seek(index);
    }

    @Override
    public int size() {
        return charStream.size();
    }

    @Override
    public String getSourceName() {
        return charStream.getSourceName();
    }
}
