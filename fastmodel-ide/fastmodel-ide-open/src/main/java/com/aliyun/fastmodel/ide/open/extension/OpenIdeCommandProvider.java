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

package com.aliyun.fastmodel.ide.open.extension;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import com.aliyun.fastmodel.converter.util.GenericTypeUtil;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.ide.spi.command.DefaultCommandProvider;
import com.aliyun.fastmodel.ide.spi.command.IdeCommand;
import com.aliyun.fastmodel.ide.spi.exception.CommandProviderGetException;
import com.aliyun.fastmodel.ide.spi.receiver.IdePlatform;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * OpenIdeCommandProvider
 *
 * @author panguanjing
 * @date 2021/12/4
 */
@Component
public class OpenIdeCommandProvider extends DefaultCommandProvider implements ApplicationContextAware {

    private ApplicationContext applicationContext;

    private Multimap<String, IdeCommand> commandMapping = ArrayListMultimap.create(16, 4);

    public OpenIdeCommandProvider(IdePlatform idePlatform) {
        super(idePlatform);
    }

    @PostConstruct
    public void init() {
        Map<String, IdeCommand> beansOfType = applicationContext.getBeansOfType(IdeCommand.class);
        for (IdeCommand command : beansOfType.values()) {
            Class superClassGenericType = GenericTypeUtil.getSuperClassGenericType(command.getClass(), 1);
            command.register(this.getIdePlatform());
            commandMapping.put(superClassGenericType.getName(), command);
        }
    }

    @Override
    public IdeCommand getIdeCommand(BaseStatement baseStatement) {
        Collection<IdeCommand> ideCommands = commandMapping.get(baseStatement.getClass().getName());
        List<IdeCommand> collect = ideCommands.stream().filter(cmd -> {
            return cmd.isMatch(baseStatement);
        }).collect(Collectors.toList());
        if (collect.isEmpty()) {
            return super.getIdeCommand(baseStatement);
        }
        if (collect.size() > 1) {
            throw new CommandProviderGetException("get command over one size:" + baseStatement.getClass().getName());
        }
        return collect.get(0);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
