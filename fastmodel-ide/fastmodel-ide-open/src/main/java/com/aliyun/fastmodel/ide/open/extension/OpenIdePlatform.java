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

package com.aliyun.fastmodel.ide.open.extension;

import com.aliyun.fastmodel.ide.spi.receiver.DocumentService;
import com.aliyun.fastmodel.ide.spi.receiver.IdePlatform;
import com.aliyun.fastmodel.ide.spi.receiver.ToolService;
import com.aliyun.fastmodel.ide.spi.receiver.impl.DefaultDocumentServiceImpl;
import com.aliyun.fastmodel.ide.spi.receiver.impl.DefaultToolServiceImpl;
import org.springframework.stereotype.Component;

/**
 * Ide platform 统计
 *
 * @author panguanjing
 * @date 2022/1/12
 */
@Component
public class OpenIdePlatform implements IdePlatform {

    private final ToolService defaultToolService;

    private final DocumentService defaultDocumentService;

    public OpenIdePlatform() {
        this.defaultToolService = new DefaultToolServiceImpl();
        this.defaultDocumentService = new DefaultDocumentServiceImpl();
    }

    @Override
    public ToolService getToolService() {
        return defaultToolService;
    }

    @Override
    public DocumentService getDocumentService() {
        return defaultDocumentService;
    }
}
