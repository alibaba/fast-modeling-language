/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.datatype.simple;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.google.common.base.Preconditions;

/**
 * simple dataType name converter
 *
 * @author panguanjing
 * @date 2022/11/7
 */
public interface SimpleDataTypeNameConverter {

    /**
     * convert to dataTypeName, if can't convert then return defaultTypeName
     * @param dataTypeName
     * @return
     */
    SimpleDataTypeName convert(String dataTypeName);


    /**
     * convert idataTypeName to simpleDataTypeName
     * @param dataTypeName
     * @return
     */
    default SimpleDataTypeName convert(IDataTypeName dataTypeName){
        Preconditions.checkNotNull(dataTypeName, "dataTypeName can't be null");
        return convert(dataTypeName.getValue());
    }
}
