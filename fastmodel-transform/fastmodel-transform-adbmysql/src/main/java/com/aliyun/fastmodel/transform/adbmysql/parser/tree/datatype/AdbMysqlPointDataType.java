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

package com.aliyun.fastmodel.transform.adbmysql.parser.tree.datatype;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.transform.adbmysql.format.AdbMysqlExpressionVisitor;
import com.aliyun.fastmodel.transform.adbmysql.parser.tree.AdbMysqlDataTypeName;
import com.aliyun.fastmodel.transform.adbmysql.parser.visitor.AdbMysqlAstVisitor;
import lombok.Getter;

/**
 * `c17` point delimiter_tokenizer ' ' COMMENT '',
 *
 * @author panguanjing
 * @date 2024/12/7
 */
@Getter
public class AdbMysqlPointDataType extends BaseDataType {
    /**
     * 分隔符
     */
    private final StringLiteral delimiterTokenizer;

    public AdbMysqlPointDataType(StringLiteral delimiterTokenizer) {
        this.delimiterTokenizer = delimiterTokenizer;
    }

    @Override
    public IDataTypeName getTypeName() {
        return AdbMysqlDataTypeName.POINT;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        AdbMysqlAstVisitor<R, C> adbMysqlAstVisitor = (AdbMysqlAstVisitor<R, C>)visitor;
        return adbMysqlAstVisitor.visitAdbMysqlPointDataType(this, context);
    }

    @Override
    public String toString() {
        return new AdbMysqlExpressionVisitor().process(this);
    }
}
