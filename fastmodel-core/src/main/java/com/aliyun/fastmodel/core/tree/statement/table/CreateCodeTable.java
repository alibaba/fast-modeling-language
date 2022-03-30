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

package com.aliyun.fastmodel.core.tree.statement.table;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import lombok.Getter;

/**
 * 使用码表信息内容。
 *
 * @author panguanjing
 * @date 2021/1/11
 */
@Getter
public class CreateCodeTable extends CreateTable {

    public static CodeTableBuilder builder() {
        return new CodeTableBuilder();
    }

    private CreateCodeTable(CodeTableBuilder codeTableBuilder) {
        super(codeTableBuilder);
    }

    public static class CodeTableBuilder extends TableBuilder<CodeTableBuilder> {

        public CodeTableBuilder() {
            detailType = TableDetailType.CODE;
        }

        @Override
        public CreateCodeTable build() {
            return new CreateCodeTable(this);
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateCodeTable(this, context);
    }
}
