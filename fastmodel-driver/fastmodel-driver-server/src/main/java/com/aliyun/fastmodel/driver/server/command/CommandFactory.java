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

package com.aliyun.fastmodel.driver.server.command;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import lombok.extern.slf4j.Slf4j;
import org.reflections.Reflections;

/**
 * @author panguanjing
 * @date 2020/12/9
 */
@Slf4j
public class CommandFactory {

    private final Map<String, FmlCommand> collection = new HashMap<>();

    private CommandFactory() {
        Reflections reflections = new Reflections("com.aliyun.fastmodel.driver.server.command");
        Set<Class<? extends FmlCommand>> subTypesOf = reflections.getSubTypesOf(FmlCommand.class);
        for (Class<? extends FmlCommand> clazz : subTypesOf) {
            try {
                Command annotation = clazz.getAnnotation(Command.class);
                if (annotation == null) {
                    log.warn("FmlCommand haven't add annotation, so will not register");
                    continue;
                }
                FmlCommand fmlCommand = (FmlCommand)clazz.getDeclaredConstructors()[0].newInstance();
                Class[] classes = annotation.value();
                for (Class c : classes) {
                    collection.put(c.getName(), fmlCommand);
                }
            } catch (InstantiationException e) {
                log.error("instantiation exception", e);
            } catch (IllegalAccessException e) {
                log.error("illegal access exception", e);
            } catch (InvocationTargetException e) {
                log.error("invocation target exception", e);
            }
        }
        log.info("init command with:{}", collection);
    }

    public static final CommandFactory INSTANCE = new CommandFactory();

    public FmlCommand getCommand(BaseStatement statement) {
        return collection.get(statement.getClass().getName());
    }

}
