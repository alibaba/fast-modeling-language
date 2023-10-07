package com.aliyun.fastmodel.driver.client.command.sample;

import java.io.ByteArrayInputStream;
import java.util.Properties;

import com.alibaba.fastjson.JSON;

import com.aliyun.fastmodel.driver.client.command.ExecuteCommand;
import com.aliyun.fastmodel.driver.client.exception.CommandException;
import com.aliyun.fastmodel.driver.client.request.FastModelWrapperResponse;
import com.aliyun.fastmodel.driver.client.request.FastModelWrapperResponse.RequestContext;
import com.aliyun.fastmodel.driver.model.DriverResult;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/4/30
 */
public class SampleExecuteCommand implements ExecuteCommand<SampleCommandProperties> {
    @Override
    public void init() throws Exception {

    }

    @Override
    public FastModelWrapperResponse execute(String sql) throws CommandException {
        DriverResult<String> driverResult = new DriverResult<>();
        driverResult.setSuccess(true);
        driverResult.setData("success");
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(JSON.toJSONBytes(driverResult));
        return new FastModelWrapperResponse(byteArrayInputStream, new RequestContext());
    }

    @Override
    public void close() throws CommandException {

    }

    @Override
    public SampleCommandProperties getProperties() {
        Properties properties = new Properties();
        return new SampleCommandProperties(properties);
    }
}
