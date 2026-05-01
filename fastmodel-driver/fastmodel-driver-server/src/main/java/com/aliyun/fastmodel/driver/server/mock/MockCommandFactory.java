package com.aliyun.fastmodel.driver.server.mock;

import java.util.Properties;

import com.aliyun.fastmodel.driver.client.command.CommandFactory;
import com.aliyun.fastmodel.driver.client.command.ExecuteCommand;
import com.google.auto.service.AutoService;

/**
 * for test
 *
 * @author panguanjing
 * @date 2022/4/30
 */
@AutoService(CommandFactory.class)
public class MockCommandFactory implements CommandFactory {

    @Override
    public ExecuteCommand createStrategy(String commandType, Properties properties) {
        return new MockExecuteCommand(new MockCommandProperties(properties));
    }
}
