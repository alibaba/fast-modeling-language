/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.core.tree.statement.table;

import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;

/**
 * ods Table信息表的内容
 *
 * @author panguanjing
 * @date 2023/6/27
 */
public class CreateOdsTable extends CreateTable {

    private CreateOdsTable(OdsTableBuilder odsTableBuilder) {
        super(odsTableBuilder);
    }

    public static OdsTableBuilder builder() {
        return new OdsTableBuilder();
    }

    public static class OdsTableBuilder extends TableBuilder<OdsTableBuilder> {

        public OdsTableBuilder() {
            detailType = TableDetailType.ODS;
        }

        @Override
        public CreateOdsTable build() {
            return new CreateOdsTable(this);
        }
    }

}

