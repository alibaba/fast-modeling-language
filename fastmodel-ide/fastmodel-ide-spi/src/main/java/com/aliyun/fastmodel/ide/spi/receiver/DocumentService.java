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

package com.aliyun.fastmodel.ide.spi.receiver;

import com.aliyun.fastmodel.ide.spi.invoker.InvokeResult;
import com.aliyun.fastmodel.ide.spi.params.DocumentFormatParams;

/**
 * 文本相关服务，针对当前IDE中的编辑的服务接口
 *
 * <pre>
 *     提供一些比如格式化操作操作，hover等一系列服务内容
 * </pre>
 *
 * @author panguanjing
 * @date 2022/1/12
 */
public interface DocumentService {
    /**
     * 格式化信息
     *
     * @param documentFormatParams
     * @return {@link InvokeResult}
     */
    InvokeResult<String> format(DocumentFormatParams documentFormatParams);
}
