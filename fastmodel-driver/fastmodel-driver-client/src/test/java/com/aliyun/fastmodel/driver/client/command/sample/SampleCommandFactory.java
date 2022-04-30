package com.aliyun.fastmodel.driver.client.command.sample;

import java.util.Properties;

import com.aliyun.fastmodel.driver.client.command.CommandFactory;
import com.aliyun.fastmodel.driver.client.command.ExecuteCommand;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/4/30
 */
public class SampleCommandFactory implements CommandFactory {
    @Override
    public ExecuteCommand createStrategy(String commandType, Properties properties) {
        return new SampleExecuteCommand();
    }
}
