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

package com.aliyun.fastmodel.transform.template;

import com.aliyun.fastmodel.transform.template.exception.FmlTemplateException;

/**
 * 引用的模版对象
 *
 * @author panguanjing
 * @date 2020/10/19
 */
public interface FmlTemplate {
    /**
     * 处理下模版信息处理。
     *
     * @param dataModel 数据对象
     * @return 解析后的文本内容
     * @throws FmlTemplateException 解析异常
     */
    String process(Object dataModel) throws FmlTemplateException;
}
