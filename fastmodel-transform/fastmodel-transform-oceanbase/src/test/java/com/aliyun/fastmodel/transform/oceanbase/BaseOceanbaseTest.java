package com.aliyun.fastmodel.transform.oceanbase;

import java.nio.charset.Charset;

import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/2/7
 */
public abstract class BaseOceanbaseTest {
    @SneakyThrows
    public String getText(String name) {
        return IOUtils.resourceToString("/oceanbase/" + name, Charset.defaultCharset());
    }
}
