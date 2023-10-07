package com.aliyun.fastmodel.driver.server.mock;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import com.alibaba.fastjson.JSON;

import com.aliyun.fastmodel.driver.client.command.ExecuteCommand;
import com.aliyun.fastmodel.driver.client.exception.CommandException;
import com.aliyun.fastmodel.driver.client.exception.FastModelException;
import com.aliyun.fastmodel.driver.client.exception.IllegalConfigException;
import com.aliyun.fastmodel.driver.client.request.FastModelRequestBody;
import com.aliyun.fastmodel.driver.client.request.FastModelWrapperResponse;
import com.aliyun.fastmodel.driver.client.request.FastModelWrapperResponse.RequestContext;
import com.aliyun.fastmodel.driver.client.utils.RequestIdUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;

import static com.aliyun.fastmodel.driver.client.FastModelEngineStatement.REQUEST_ID;
import static com.aliyun.fastmodel.driver.client.FastModelEngineStatement.SQL;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/4/30
 */
public class MockExecuteCommand implements ExecuteCommand<MockCommandProperties> {
    private final MockCommandProperties sampleCommandProperties;
    CloseableHttpClient httpClient = null;

    @Override
    public void init() throws Exception {
        httpClient = new SampleHttpClientBuilder(this.getProperties()).buildClient();
    }

    public MockExecuteCommand(MockCommandProperties sampleCommandProperties) {
        this.sampleCommandProperties = sampleCommandProperties;
    }

    @Override
    public FastModelWrapperResponse execute(String sql) throws CommandException {
        URI uri = buildRequestUri();
        return getResponse(uri, sql);
    }

    private FastModelWrapperResponse getResponse(URI uri, String sql)
        throws FastModelException {
        String nextRequestId = RequestIdUtils.createNextRequestId();
        try {
            HttpPost post = new HttpPost(uri);
            post.setHeader("content-type", "application/json");
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
            throw new FastModelException(e, this.getProperties().getHost()).addContextValue(REQUEST_ID, nextRequestId).addContextValue(
                SQL, sql
            );
        }
    }

    private URI buildRequestUri() {
        try {
            MockCommandProperties properties = this.getProperties();
            String host = properties.getHost();
            String path = "/" + properties.getDatabase();
            URIBuilder builder = new URIBuilder();
            URIBuilder uriBuilder = builder.setScheme("http").setHost(host);
            if (properties.getPort() != null && properties.getPort() != 0) {
                uriBuilder.setPort(properties.getPort());
            }
            return uriBuilder.setPath(path).build();
        } catch (Exception e) {
            throw new IllegalConfigException("config is illegal", e, this.getProperties().getHost());
        }
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
    public MockCommandProperties getProperties() {
        return this.sampleCommandProperties;
    }
}
