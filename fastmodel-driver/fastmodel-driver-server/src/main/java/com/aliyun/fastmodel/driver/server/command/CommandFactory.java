/*
 * Copyright (c)  2020. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.driver.server.command;

import java.util.HashMap;
import java.util.Map;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import lombok.extern.slf4j.Slf4j;

/**
 * @author panguanjing
 * @date 2020/12/9
 */
@Slf4j
public class CommandFactory {

    private final Map<String, FmlCommand> collection = new HashMap<>();

    private CommandFactory() {
        CreateTableCommand createTableCommand = new CreateTableCommand();
        init(createTableCommand);
        ShowObjectCommand showObjectCommand = new ShowObjectCommand();
        init(showObjectCommand);
        log.info("init command with:{}", collection);
    }

    private void init(FmlCommand createTableCommand) {
        Command fmlCommand = createTableCommand.getClass().getAnnotation(Command.class);
        Class[] value = fmlCommand.value();
        for (Class c : value) {
            collection.put(c.getName(), createTableCommand);
        }
    }

    public static final CommandFactory INSTANCE = new CommandFactory();

    public FmlCommand getCommand(BaseStatement statement) {
        return collection.get(statement.getClass().getName());
    }

}
