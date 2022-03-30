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

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.ide.spi.invoker.InvokeResult;
import com.aliyun.fastmodel.ide.spi.receiver.IdePlatform;

/**
 * 执行command
 *
 * @author panguanjing
 * @date 2021/12/3
 */
public interface IdeCommand<T, S extends BaseStatement> {
    /**
     * 执行结果内容处理
     *
     * @param statement 执行的FML语句
     * @return {@link InvokeResult}
     */
    public InvokeResult<T> execute(S statement);

    /**
     * 是否match当前的statement信息
     *
     * @param statement
     * @return
     */
    public boolean isMatch(S statement);

    /**
     * 注册platform
     *
     * @param idePlatform
     */
    public void register(IdePlatform idePlatform);
}
