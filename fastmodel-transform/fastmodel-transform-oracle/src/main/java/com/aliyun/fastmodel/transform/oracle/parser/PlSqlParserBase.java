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

package com.aliyun.fastmodel.transform.oracle.parser;

import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.TokenStream;

/**
 * use for antlr generator class
 *
 * @author panguanjing
 */
public abstract class PlSqlParserBase extends Parser {
    private boolean isVersion12 = true;
    private boolean isVersion10 = true;

    public PlSqlParserBase(TokenStream input) {
        super(input);
    }

    public boolean isVersion12() {
        return isVersion12;
    }

    public void setVersion12(boolean value) {
        isVersion12 = value;
    }

    public boolean isVersion10() {
        return isVersion10;
    }

    public void setVersion10(boolean value) {
        isVersion10 = value;
    }
}
