/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.parser.impl;

import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.CreateOdsTable;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/3/6
 */
public class CreateOdsTableTest extends BaseTest {

    @Test
    public void testCreateOds1() {
        String fml = "CREATE ODS TABLE IF NOT EXISTS ods_shop_1 "
            + "(a BIGINT REL_DIMENSION REFERENCES dim_shop.shop_code, b STRING REL_INDICATOR REFERENCES (ind1,ind2), "
            + "c DATE STAT_TIME WITH ('pattern'='yyyymmdd')) "
            + "COMMENT 'comment'";
        CreateOdsTable createOdsTable = parse(fml, CreateOdsTable.class);
        assertEquals(createOdsTable.toString(), "CREATE ODS TABLE IF NOT EXISTS ods_shop_1 \n"
            + "(\n"
            + "   a BIGINT REL_DIMENSION REFERENCES dim_shop.shop_code,\n"
            + "   b STRING REL_INDICATOR REFERENCES (ind1,ind2),\n"
            + "   c DATE STAT_TIME WITH ('pattern'='yyyymmdd')\n"
            + ")\n"
            + "COMMENT 'comment'");
    }

    @Test
    public void testCreateOds1TableConstraint() {
        String fml = "CREATE ODS TABLE IF NOT EXISTS ods_shop_1 "
            + "("
            + "\n a BIGINT REL_DIMENSION REFERENCES dim_shop.shop_code, "
            + "\n b STRING REL_INDICATOR REFERENCES (ind1,ind2), "
            + "\n CONSTRAINT c2 TIME_PERIOD KEY REFERENCES (d1, d2)"
            + ") "
            + "COMMENT 'comment'";
        CreateOdsTable createOdsTable = parse(fml, CreateOdsTable.class);
        assertEquals(createOdsTable.toString(), "CREATE ODS TABLE IF NOT EXISTS ods_shop_1 \n"
            + "(\n"
            + "   a BIGINT REL_DIMENSION REFERENCES dim_shop.shop_code,\n"
            + "   b STRING REL_INDICATOR REFERENCES (ind1,ind2),\n"
            + "   CONSTRAINT c2 TIME_PERIOD KEY REFERENCES (d1,d2)\n"
            + ")\n"
            + "COMMENT 'comment'");
    }

    @Test
    public void testCreateOdsTable() {
        String sql = "CREATE ODS TABLE IF NOT EXISTS test_bu.ods_shop_sku_sales_d1 (\n"
            + "  gmt_create string COMMENT '创建时间' WITH ('pattern'='yyyy-MM-dd HH:mm:ss'),\n"
            + "  sku_code string COMMENT '商品code' WITH('time_period'='code', 'indicator'='indicator_key'),\n"
            + "  shop_code string COMMENT '门店code',\n"
            + "  sku_type string COMMENT '商品类型',\n"
            + "  sku_name string COMMENT '商品name',\n"
            + "  brand_code string COMMENT '品牌code',\n"
            + "  dept_code string COMMENT '部门code',\n"
            + "  pay_amount string COMMENT '1级类目id',\n"
            + "  pay_cnt string COMMENT '2级类目id',\n"
            + "  pay_user_cnt string COMMENT '3级类目id',\n"
            + "  new_pay_user_cnt string COMMENT '4级类目id',\n"
            + "\n"
            + "  PRIMARY KEY (sku_code, shop_code), /* 自动根据维度表主键生成 */\n"
            + "  CONSTRAINT rel_dim_shop DIM KEY (shop_code) REFERENCES dim_shop(shop_code),\n"
            + "  CONSTRAINT rel_dim_sku DIM KEY (shop_code, sku_code) REFERENCES dim_sku(shop_code, sku_code)\n"
            + ") COMMENT '门店商品销售域指标表' WITH (\n"
            + "  'data_domain' = 'test_dm',\n"
            + "  'data_layer' = 'ods'\n"
            + ");";
        CreateOdsTable parse = parse(sql, CreateOdsTable.class);

        assertEquals(parse.toString(), "CREATE ODS TABLE IF NOT EXISTS test_bu.ods_shop_sku_sales_d1 \n"
            + "(\n"
            + "   gmt_create       STRING COMMENT '创建时间' WITH ('pattern'='yyyy-MM-dd HH:mm:ss'),\n"
            + "   sku_code         STRING NOT NULL COMMENT '商品code' WITH ('time_period'='code',"
            + "'indicator'='indicator_key'),\n"
            + "   shop_code        STRING NOT NULL COMMENT '门店code',\n"
            + "   sku_type         STRING COMMENT '商品类型',\n"
            + "   sku_name         STRING COMMENT '商品name',\n"
            + "   brand_code       STRING COMMENT '品牌code',\n"
            + "   dept_code        STRING COMMENT '部门code',\n"
            + "   pay_amount       STRING COMMENT '1级类目id',\n"
            + "   pay_cnt          STRING COMMENT '2级类目id',\n"
            + "   pay_user_cnt     STRING COMMENT '3级类目id',\n"
            + "   new_pay_user_cnt STRING COMMENT '4级类目id',\n"
            + "   PRIMARY KEY(sku_code,shop_code),\n"
            + "   CONSTRAINT rel_dim_shop DIM KEY (shop_code) REFERENCES dim_shop (shop_code),\n"
            + "   CONSTRAINT rel_dim_sku DIM KEY (shop_code,sku_code) REFERENCES dim_sku (shop_code,sku_code)\n"
            + ")\n"
            + "COMMENT '门店商品销售域指标表'\n"
            + "WITH('data_domain'='test_dm','data_layer'='ods')");

    }

    @Test
    public void testAlterOdsTable() {
        String fml = "ALTER TABLE dim_shop ADD COLUMNS (a BIGINT REL_DIMENSION REFERENCES dim_shop.abc)";
        AddCols parse = parse(fml, AddCols.class);
        assertEquals(parse.toString(),
            "ALTER TABLE dim_shop ADD COLUMNS\n"
                + "(\n"
                + "   a BIGINT REL_DIMENSION REFERENCES dim_shop.abc\n"
                + ")");
    }

    @Test
    public void testChangeOdsTableCol() {
        String fml = "ALTER TABLE dim_shop CHANGE COLUMN c1 c2 BIGINT REL_DIMENSION REFERENCES dim_shop.abc";
        ChangeCol changeCol = parse(fml, ChangeCol.class);
        assertEquals(changeCol.toString(),
            "ALTER TABLE dim_shop CHANGE COLUMN c1 c2 BIGINT REL_DIMENSION REFERENCES dim_shop.abc");
    }
}
