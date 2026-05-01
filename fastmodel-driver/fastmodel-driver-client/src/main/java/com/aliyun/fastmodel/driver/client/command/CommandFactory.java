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

import java.util.Properties;

/**
 * 用于Command的工厂类
 *
 * @author panguanjing
 * @date 2021/3/25
 */
public interface CommandFactory {

    /**
     * 获得执行的命令对象
     *
     * @param commandType 命令类型
     * @param properties  属性信息
     * @return
     */
    public ExecuteCommand createStrategy(String commandType, Properties properties);
}
