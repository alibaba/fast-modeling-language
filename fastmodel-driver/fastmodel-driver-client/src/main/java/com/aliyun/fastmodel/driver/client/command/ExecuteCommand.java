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

package com.aliyun.fastmodel.driver.client.command;

import com.aliyun.fastmodel.driver.client.exception.CommandException;
import com.aliyun.fastmodel.driver.client.request.FastModelWrapperResponse;

/**
 * AuthorizationStrategy  验权策略
 *
 * @author panguanjing
 * @date 2021/3/25
 */
public interface ExecuteCommand<T extends BaseCommandProperties> {

    /**
     * 初始化的操作内容
     *
     * @throws Exception
     */
    void init() throws Exception;

    /**
     * 执行的结果内容
     *
     * @param sql
     * @return
     * @throws CommandException
     */
    FastModelWrapperResponse execute(String sql) throws CommandException;

    /**
     * 关闭信息
     *
     * @throws CommandException
     */
    void close() throws CommandException;

    /**
     * 获取配置信息
     *
     * @return 返回属性
     */
    T getProperties();
}
