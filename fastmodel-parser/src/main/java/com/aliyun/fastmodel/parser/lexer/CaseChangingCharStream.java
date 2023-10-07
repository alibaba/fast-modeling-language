/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.parser.lexer;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;

/**
 * Desc:
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
