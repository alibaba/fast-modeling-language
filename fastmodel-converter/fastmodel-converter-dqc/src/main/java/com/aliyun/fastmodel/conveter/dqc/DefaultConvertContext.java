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

package com.aliyun.fastmodel.conveter.dqc;

import com.aliyun.fastmodel.converter.spi.ConvertContext;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import lombok.Setter;

/**
 * convert context
 *
 * @author panguanjing
 * @date 2021/6/23
 */
@Setter
public class DefaultConvertContext implements ConvertContext {

    private BaseStatement beforeStatement;

    private BaseStatement afterStatement;

    @Override
    public BaseStatement getBeforeStatement() {
        return beforeStatement;
    }

    @Override
    public BaseStatement getAfterStatement() {
        return afterStatement;
    }
}
