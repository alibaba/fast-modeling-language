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

package com.aliyun.fastmodel.driver.client.exception;

import org.apache.commons.lang3.exception.ContextedRuntimeException;

/**
 * 对SQLException的扩展
 *
 * @author panguanjing
 */
public class FastModelException extends ContextedRuntimeException {

    public FastModelException(Throwable cause, String url) {
        this(cause.getMessage(), cause, url);
    }

    public FastModelException(String message, Throwable cause, String url) {
        super(message, cause);
        addContextValue("url", url);
    }

}
