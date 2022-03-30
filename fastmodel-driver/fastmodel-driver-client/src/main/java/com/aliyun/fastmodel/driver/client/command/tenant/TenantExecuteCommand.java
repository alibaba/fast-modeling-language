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

package com.aliyun.fastmodel.driver.client.command.tenant;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;

import com.aliyun.fastmodel.driver.client.command.ExecuteCommand;
import com.aliyun.fastmodel.driver.client.exception.CommandException;
import com.aliyun.fastmodel.driver.client.exception.FastModelException;
import com.aliyun.fastmodel.driver.client.exception.IllegalConfigException;
import com.aliyun.fastmodel.driver.client.request.FastModelRequestBody;
import com.aliyun.fastmodel.driver.client.request.FastModelWrapperResponse;
import com.aliyun.fastmodel.driver.client.request.FastModelWrapperResponse.RequestContext;
import com.aliyun.fastmodel.driver.client.utils.RequestIdUtils;
import com.aliyun.fastmodel.driver.client.utils.Version;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;

import static com.aliyun.fastmodel.driver.client.FastModelEngineStatement.REQUEST_ID;
import static com.aliyun.fastmodel.driver.client.FastModelEngineStatement.SQL;

/**
 * 执行的信息
 *
 * @author panguanjing
 * @date 2021/3/25
 */
public class TenantExecuteCommand implements ExecuteCommand<TenantProperties> {

    public static final String HTTP = "http";
    public static final String HTTPS = "https";

    public static final String X_REQUEST_ID = "x-fml-request-id";
    public static final String FML_BASE_ID = "x-fml-base-id";
    public static final String FML_TENANT_ID = "x-fml-tenant-id";
    public static final String FML_PROJECT_ID = "x-fml-project-id";
    public static final String FML_BU_CODE = "x-fml-bu-code";
    public static final String FML_SHOW_NAME = "x-fml-show-name";

    private TenantProperties properties;

    CloseableHttpClient httpClient = null;

    public TenantExecuteCommand(TenantProperties tenantProperties) {
        properties = tenantProperties;
    }

    @Override
    public void init() throws Exception {
        httpClient = new TenantHttpClientBuilder(properties).buildClient();
    }

    @Override
    public FastModelWrapperResponse execute(String sql) throws CommandException {
        List<NameValuePair> nameValuePairs = getNameValuePairs(properties);
        return getResponse(httpClient, nameValuePairs, sql);
    }

    private FastModelWrapperResponse getResponse(CloseableHttpClient httpClient, List<NameValuePair> nameValuePairs,
                                                 String sql)
        throws FastModelException {
        String host = properties.getHost();
        String nextRequestId = RequestIdUtils.createNextRequestId();
        try {
            URI uri = buildRequestUri(nameValuePairs);
            HttpPost post = new HttpPost(uri);
            String signature = TenantTokenUtil.getSignature(properties.getToken(), uri.getRawQuery());
            post.setHeader("signature", signature);
            post.setHeader("content-type", "application/json");
            //上下文参数
            post.setHeader(FML_BASE_ID, properties.getUser());
            post.setHeader(FML_TENANT_ID, properties.getTenantId());
            post.setHeader(FML_PROJECT_ID, properties.getDatabase());
            post.setHeader(FML_BU_CODE, properties.getBuCode());
            String showName = properties.getShowName();
            if (StringUtils.isNotBlank(showName)) {
                post.setHeader(FML_SHOW_NAME, URLEncoder.encode(showName, "UTF-8"));
            }
            //model-engine依赖这个header头注入requestId
            post.setHeader(X_REQUEST_ID, nextRequestId);
            FastModelRequestBody requestBody = new FastModelRequestBody();
            requestBody.setFml(sql);
            post.setEntity(new StringEntity(JSON.toJSONString(requestBody), StandardCharsets.UTF_8));
            CloseableHttpResponse execute = httpClient.execute(post);
            HttpEntity entity = execute.getEntity();
            InputStream is = entity.getContent();
            RequestContext requestContext = new RequestContext();
            requestContext.setRequestId(nextRequestId);
            requestContext.setRequestSql(sql);
            FastModelWrapperResponse response = new FastModelWrapperResponse(
                is, requestContext
            );
            return response;
        } catch (Throwable e) {
            throw new FastModelException(e, host).addContextValue(REQUEST_ID, nextRequestId).addContextValue(
                SQL, sql
            );
        }
    }

    public URI buildRequestUri(List<NameValuePair> nameValuePairs) {
        try {
            String host = properties.getHost();
            String path = properties.getPath() == null || properties.getPath().isEmpty() ? "/" : properties
                .getPath();
            URIBuilder builder = new URIBuilder();
            URIBuilder uriBuilder = builder.setScheme(properties.getSsl() ? HTTPS : HTTP).setHost(host
            );
            if (properties.getPort() != null && properties.getPort() != 0) {
                uriBuilder.setPort(properties.getPort());
            }
            return uriBuilder.setPath(
                path).setParameters(nameValuePairs).build();
        } catch (Exception e) {
            throw new IllegalConfigException("config is illegal", e, properties.getHost());
        }
    }

    private List<NameValuePair> getNameValuePairs(TenantProperties properties) {
        List<NameValuePair> result = new ArrayList<>();
        QueryParams[] queryParams = QueryParams.values();
        for (QueryParams q : queryParams) {
            String valuePair = properties.asProperty().getProperty(q.getKey());
            if (valuePair == null && q.getDefaultValue() != null) {
                result.add(new BasicNameValuePair(q.getKey(), String.valueOf(q.getDefaultValue())));
            } else {
                result.add(new BasicNameValuePair(q.getKey(), valuePair));
            }
        }
        Long timestamp = System.currentTimeMillis();
        result.add(new BasicNameValuePair("timestamp", String.valueOf(timestamp)));
        result.add(new BasicNameValuePair("version", Version.getVersion()));
        return result;
    }

    @Override
    public void close() throws CommandException {
        try {
            httpClient.close();
        } catch (IOException e) {
            //ignore
        }
    }

    @Override
    public TenantProperties getProperties() {
        return properties;
    }

}
