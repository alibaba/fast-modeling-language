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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.converter.util.GenericTypeUtil;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.BaseCommandStatement;
import com.aliyun.fastmodel.ide.spi.exception.CommandProviderGetException;
import com.aliyun.fastmodel.ide.spi.receiver.IdePlatform;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.Getter;

/**
 * OpenIdeCommandProvider
 *
 * @author panguanjing
 * @date 2021/12/4
 */
public class DefaultCommandProvider implements CommandProvider {

    private Multimap<String, IdeCommand> commandMapping = ArrayListMultimap.create(16, 4);

    @Getter
    private final IdePlatform idePlatform;

    public DefaultCommandProvider(IdePlatform idePlatform) {
        this.idePlatform = idePlatform;
        init();
    }

    private void init() {
        Iterator<IdeCommand> beansOfType = ServiceLoader.load(IdeCommand.class).iterator();
        for (Iterator<IdeCommand> it = beansOfType; it.hasNext(); ) {
            IdeCommand command = it.next();
            command.register(idePlatform);
            Class superClassGenericType = GenericTypeUtil.getSuperClassGenericType(command.getClass(), 1);
            commandMapping.put(superClassGenericType.getName(), command);
        }
    }

    @Override
    public IdeCommand getIdeCommand(BaseStatement baseStatement) {
        boolean isCommandStatement = baseStatement instanceof BaseCommandStatement;
        if (!isCommandStatement) {
            throw new CommandProviderGetException("statement is not command");
        }
        BaseCommandStatement baseCommandStatement = (BaseCommandStatement)baseStatement;
        Collection<IdeCommand> ideCommands = commandMapping.get(baseCommandStatement.getClass().getName());
        List<IdeCommand> collect = ideCommands.stream().filter(cmd -> {
            return cmd.isMatch(baseCommandStatement);
        }).collect(Collectors.toList());
        if (collect.isEmpty()) {
            throw new CommandProviderGetException("get command is zero:" + baseStatement.getClass().getName());
        }
        if (collect.size() > 1) {
            throw new CommandProviderGetException("get command over one size:" + baseStatement.getClass().getName());
        }
        return collect.get(0);
    }
}
