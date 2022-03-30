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

package com.aliyun.fastmodel.ide.spi.invoker;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * ExecutorResult
 *
 * @author panguanjing
 * @date 2021/9/29
 */
@Getter
@Setter
@Builder
public class InvokeResult<T> {

    /**
     * 是否成功
     */
    private boolean success;

    /**
     * 错误码 {@link com.aliyun.fastmodel.ide.spi.exception.error.IErrorCode}
     */
    private String errorCode;

    /**
     * 错误消息
     */
    private String message;

    /**
     * 结果数据
     */
    private T data;

    /**
     * 快捷方法，成功对象构造
     *
     * @param data
     * @param <T>
     * @return
     */
    public static <T> InvokeResult<T> success(T data) {
        return InvokeResult.<T>builder()
            .success(true)
            .data(data)
            .build();
    }

    /**
     * 错误方法构造
     *
     * @param errorCode
     * @param message
     * @param <T>
     * @return
     */
    public static <T> InvokeResult<T> error(String errorCode, String message) {
        return InvokeResult.<T>builder()
            .success(false)
            .errorCode(errorCode)
            .message(message)
            .build();
    }

    /**
     * 错误方法构造，只有错误码
     *
     * @param errorCode
     * @param <T>
     * @return
     */
    public static <T> InvokeResult<T> error(String errorCode) {
        return InvokeResult.<T>builder()
            .success(false)
            .errorCode(errorCode)
            .build();
    }
}
