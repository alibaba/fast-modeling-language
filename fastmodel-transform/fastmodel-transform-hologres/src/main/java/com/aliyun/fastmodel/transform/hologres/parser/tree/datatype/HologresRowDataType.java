/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.parser.tree.datatype;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.VariableDataTypeName;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * hologres
 *
 * @author panguanjing
 * @date 2022/6/9
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class HologresRowDataType extends BaseDataType {

    public HologresRowDataType(QualifiedName qualifiedName,
                               RowType rowType) {
        this.qualifiedName = qualifiedName;
        this.rowType = rowType;
    }

    public enum RowType {
        /**
         * row type
         */
        ROWTYPE,
        /**
         * type
         */
        TYPE
    }

    ;

    /**
     * row data type qulifiedname
     */
    private final QualifiedName qualifiedName;

    /**
     * row type
     */
    private final RowType rowType;

    @Override
    public IDataTypeName getTypeName() {
        return new VariableDataTypeName(qualifiedName.toString());
    }
}
