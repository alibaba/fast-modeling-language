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
import com.aliyun.fastmodel.ide.spi.receiver.IdePlatform;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 基本的ide Command的处理
 *
 * @author panguanjing
 * @date 2021/12/3
 */
@Getter
@EqualsAndHashCode
public abstract class BaseIdeCommand<T, S extends BaseStatement> implements IdeCommand<T, S> {

    private IdePlatform idePlatform;

    @Override
    public void register(IdePlatform idePlatform) {
        this.idePlatform = idePlatform;
    }

    protected IdePlatform getIdePlatform() {
        return idePlatform;
    }

    /**
     * 由于一个statement命令，会有多个match的command，所以增加isMatch方法
     * 用于过滤
     *
     * @param baseStatement
     * @return true 如果命中，false如果不命中
     */
    @Override
    public boolean isMatch(S baseStatement) {
        return true;
    }

}
