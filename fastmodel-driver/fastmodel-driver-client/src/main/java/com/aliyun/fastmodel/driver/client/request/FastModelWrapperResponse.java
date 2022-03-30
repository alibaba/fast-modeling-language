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

package com.aliyun.fastmodel.driver.client.request;

import java.io.InputStream;

import lombok.Getter;
import lombok.Setter;

/**
 * 返回结果的包装类
 *
 * @author panguanjing
 * @date 2021/3/24
 */
@Getter
public class FastModelWrapperResponse {

    /**
     * http请求的后的结果
     */
    private final InputStream response;

    private final RequestContext requestContext;

    public FastModelWrapperResponse(
        InputStream response, RequestContext requestContext) {
        this.response = response;
        this.requestContext = requestContext;
    }

    public String getRequestSql() {return requestContext.getRequestSql();}

    public String getRequestId() {return requestContext.getRequestId();}

    /**
     * 请求的上下文
     */
    public static class RequestContext {
        /**
         * 请求的sql
         */
        @Getter
        @Setter
        private String requestSql;

        /**
         * 请求的Id
         */
        @Getter
        @Setter
        private String requestId;
    }

}
