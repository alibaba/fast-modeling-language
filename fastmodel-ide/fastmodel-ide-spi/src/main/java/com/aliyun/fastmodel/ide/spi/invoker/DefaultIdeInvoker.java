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

package com.aliyun.fastmodel.ide.spi.invoker;

import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.ide.spi.command.CommandProvider;
import com.aliyun.fastmodel.ide.spi.command.IdeCommand;
import com.aliyun.fastmodel.ide.spi.params.FmlParams;

/**
 * DefaultIdeInvoker
 * <pre>
 *     默认的ideInvoker的实现，用于获取的
 * </pre>
 *
 * @author panguanjing
 * @date 2022/1/12
 */
public class DefaultIdeInvoker implements IdeInvoker {

    private CommandProvider commandProvider;

    private FastModelParser fastModelParser;

    public DefaultIdeInvoker(CommandProvider commandProvider) {
        this.commandProvider = commandProvider;
        fastModelParser = FastModelParserFactory.getInstance().get();
    }

    @Override
    public <T> InvokeResult<T> invoke(FmlParams params) {
        BaseStatement baseStatement = fastModelParser.parseStatement(params.body());
        IdeCommand ideCommand = commandProvider.getIdeCommand(baseStatement);
        return ideCommand.execute(baseStatement);
    }
}
