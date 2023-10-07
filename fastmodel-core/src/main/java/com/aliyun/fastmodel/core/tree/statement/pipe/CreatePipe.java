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

package com.aliyun.fastmodel.core.tree.statement.pipe;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.statement.BaseCreate;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.google.common.base.Preconditions;
import lombok.Getter;

/**
 * 定义一套 Pipe的语法内容
 *
 * @author panguanjing
 * @date 2021/4/6
 */
@Getter
public class CreatePipe extends BaseCreate {

    private final PipeCopyInto pipeCopyInto;

    private final PipeType pipeType;

    private final PipeFrom pipeFrom;

    public CreatePipe(CreateElement element,
                      PipeType pipeType, PipeCopyInto pipeCopyInto,
                      PipeFrom from) {
        super(element);
        Preconditions.checkNotNull(pipeCopyInto, "copy can't be null");
        Preconditions.checkNotNull(from, "from can't be null");
        this.pipeCopyInto = pipeCopyInto;
        this.pipeType = pipeType;
        pipeFrom = from;
        setStatementType(StatementType.PIPE);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreatePipe(this, context);
    }
}
