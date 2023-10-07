package com.aliyun.fastmodel.transform.api.format;

/**
 * property key
 *
 * @author panguanjing
 * @date 2023/2/11
 */
public interface PropertyKey {

    /**
     * key name
     *
     * @return
     */
    String getValue();

    /**
     * 是否支持打印到property
     *
     * @return
     */
    boolean isSupportPrint();




}
