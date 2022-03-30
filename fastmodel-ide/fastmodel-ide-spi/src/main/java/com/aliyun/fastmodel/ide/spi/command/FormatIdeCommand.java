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

package com.aliyun.fastmodel.ide.spi.command;

import com.aliyun.fastmodel.core.tree.statement.command.FormatFml;
import com.aliyun.fastmodel.ide.spi.invoker.InvokeResult;
import com.aliyun.fastmodel.ide.spi.params.DocumentFormatParams;
import com.google.auto.service.AutoService;

/**
 * Format Ide Command
 *
 * @author panguanjing
 * @date 2022/1/10
 */
@AutoService(IdeCommand.class)
public class FormatIdeCommand extends BaseIdeCommand<String, FormatFml> {

    @Override
    public InvokeResult<String> execute(FormatFml statement) {
        DocumentFormatParams documentFormatParams = DocumentFormatParams.builder()
            .text(statement.getFml().get())
            .build();
        return this.getIdePlatform().getDocumentService().format(documentFormatParams);
    }
}
