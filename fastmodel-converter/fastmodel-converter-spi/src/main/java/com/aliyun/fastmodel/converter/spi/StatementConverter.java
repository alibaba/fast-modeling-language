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

package com.aliyun.fastmodel.converter.spi;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;

/**
 * 转化器，支持FML模型之间互相转换
 *
 * @author panguanjing
 * @date 2021/5/30
 */
public interface StatementConverter<T extends BaseStatement, R extends BaseStatement, C extends ConvertContext> {

    /**
     * 从一种statement转换为另外一种statement
     *
     * @param source
     * @param context
     * @return BaseStatement
     */
    default R convert(T source, C context){
        return null;
    }

}
