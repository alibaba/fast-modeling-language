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

CREATE INDEX idx_adjunct_name ON `adjunct` (`name`);
ALTER TABLE business_process
    ADD
        (
        `parent_code` VARCHAR(128) COMMENT '父节点code'
        );
ALTER TABLE checker
    ADD
        (
        `is_default` TINYINT(4) COMMENT '是否是主校验规则',
        `expression_value` VARCHAR(2048) COMMENT '表达式的自定义的值'
        );
ALTER TABLE directory
    ADD
        (
        `scope` TEXT COMMENT '约束范围，比如：业务分类下的数据域'
        );
CREATE INDEX idx_indicator_atomic_name ON `indicator_atomic` (`name`);
ALTER TABLE model_table
    ADD
        (
        `storage_policy_code` VARCHAR(64) COMMENT '存储策略编码'
        );
ALTER TABLE online_dqc_rule
    ADD
        (
        `model_code` VARCHAR(128) COMMENT '模型code'
        );
CREATE INDEX idx_time_period_name ON `time_period` (`name`);
ALTER TABLE t_online_model
    CHANGE COLUMN `business_process_name` `business_process_name` VARCHAR(70) NULL;
ALTER TABLE t_online_model
    ADD
        (
        `source_project_id` VARCHAR(128) COMMENT '源项目id',
        `model_code` VARCHAR(128) COMMENT '模型code'
        );
ALTER TABLE t_release_package_object
    CHANGE COLUMN `need_realease_fml` `need_realease_fml` MEDIUMTEXT;
ALTER TABLE t_release_package_object
    CHANGE COLUMN `big_engine_dsl` `big_engine_dsl` MEDIUMTEXT;
ALTER TABLE t_release_package_object
    ADD
        (
        `release_object_type` VARCHAR(64) COMMENT '部署对象类型',
        `source_project_id` VARCHAR(128) COMMENT '源项目id'
        );
ALTER TABLE t_vc_commit_object
    CHANGE COLUMN `business_process_name` `business_process_name` VARCHAR(70) NULL;
ALTER TABLE t_vc_commit_object
    ADD
        (
        `model_version` INT(11) NOT NULL COMMENT '模型版本',
        `model_code` VARCHAR(128) COMMENT '模型code'
        );
CREATE TABLE config
(
    `id`                 BIGINT(20)   NOT NULL COMMENT '主键',
    `gmt_create`         DATETIME     NOT NULL COMMENT '创建时间',
    `gmt_modified`       DATETIME     NOT NULL COMMENT '修改时间',
    `namespace`          VARCHAR(128) NOT NULL COMMENT '空间',
    `m_key`              VARCHAR(256) NOT NULL COMMENT '配置分组，key是数据库保留字，不采用',
    `value`              TEXT         NOT NULL COMMENT '配置内容',
    `creator_id`         VARCHAR(64)  NOT NULL COMMENT '创建者id',
    `creator_name`       VARCHAR(64)  NOT NULL COMMENT '创建者名称',
    `modifier_id`        VARCHAR(64) COMMENT '修改者id',
    `modifier_name`      VARCHAR(64) COMMENT '修改者名称',
    `is_deleted`         TINYINT(4)   NOT NULL COMMENT '是否删除',
    `tenant_id`          VARCHAR(64)  NOT NULL COMMENT '当前租户id',
    `business_unit_uuid` VARCHAR(64)  NOT NULL COMMENT '业务板块uuid',
    PRIMARY KEY (`id`),
    INDEX `idx_namespace_key` (`tenant_id`, `business_unit_uuid`, `namespace`, `m_key`)
) COMMENT '配置中心';
CREATE TABLE design_project
(
    `id`                        BIGINT(20)   NOT NULL COMMENT '主键',
    `gmt_create`                DATETIME     NOT NULL COMMENT '创建时间',
    `gmt_modified`              DATETIME     NOT NULL COMMENT '修改时间',
    `design_project_id`         VARCHAR(64)  NOT NULL COMMENT '设计空间id',
    `project_id`                VARCHAR(64)  NOT NULL COMMENT '物化空间id',
    `modifier_id`               VARCHAR(64)  NOT NULL COMMENT '修改者id',
    `modifier_name`             VARCHAR(64)  NOT NULL COMMENT '修改者名称',
    `creator_id`                VARCHAR(64)  NOT NULL COMMENT '创建者id',
    `creator_name`              VARCHAR(64)  NOT NULL COMMENT '创建者名称',
    `is_deleted`                TINYINT(4)   NOT NULL COMMENT '是否删除',
    `project_identifier`        VARCHAR(128) NOT NULL COMMENT '项目空间标识',
    `design_project_identifier` VARCHAR(128) NOT NULL COMMENT '设计空间标识',
    `tenant_id`                 VARCHAR(64)  NOT NULL COMMENT '租户id',
    PRIMARY KEY (`id`),
    INDEX `idx_design_materialize` (`design_project_id`, `project_id`),
    INDEX `idx_materialize_design` (`project_id`, `design_project_id`)
) COMMENT '设计空间关系';
CREATE TABLE design_project_record
(
    `id`                 BIGINT(20)    NOT NULL COMMENT '主键',
    `gmt_create`         DATETIME      NOT NULL COMMENT '创建时间',
    `gmt_modified`       DATETIME      NOT NULL COMMENT '修改时间',
    `project_name`       VARCHAR(1024) NOT NULL COMMENT '工作空间中文名',
    `project_identifier` VARCHAR(128)  NOT NULL COMMENT '工作空间英文名',
    `modifier_id`        VARCHAR(128)  NOT NULL COMMENT '变更操作人id',
    `modifier_name`      VARCHAR(128)  NOT NULL COMMENT '变更人名称',
    `project_id`         VARCHAR(128)  NOT NULL COMMENT '项目空间id',
    `type`               TINYINT(4)    NOT NULL COMMENT '操作方式',
    `design_project_id`  VARCHAR(128)  NOT NULL COMMENT '设计空间id',
    PRIMARY KEY (`id`)
) COMMENT '设计空间变更记录';
CREATE TABLE distributed_config
(
    `id`           BIGINT(20) NOT NULL COMMENT '主键',
    `gmt_create`   DATETIME   NOT NULL COMMENT '创建时间',
    `gmt_modified` DATETIME   NOT NULL COMMENT '修改时间',
    `namespace`    VARCHAR(128) COMMENT '配置空间',
    `config_key`   VARCHAR(64) COMMENT '配置key',
    `config_val`   JSON COMMENT '配置value',
    `modifier_id`  VARCHAR(64) COMMENT '最近修改人id',
    PRIMARY KEY (`id`),
    UNIQUE KEY (`config_key`, `namespace`)
) COMMENT '分布式配置';
CREATE TABLE distributed_task
(
    `id`           BIGINT(20)   NOT NULL COMMENT '主键',
    `gmt_create`   DATETIME     NOT NULL COMMENT '创建时间',
    `gmt_modified` DATETIME     NOT NULL COMMENT '修改时间',
    `gmt_expire`   DATETIME COMMENT '过期时间',
    `namespace`    VARCHAR(128) NOT NULL COMMENT 'namespace',
    `task_id`      VARCHAR(64)  NOT NULL COMMENT '任务id',
    `task_type`    VARCHAR(64)  NOT NULL COMMENT '任务类型',
    `task_status`  VARCHAR(16)  NOT NULL COMMENT '任务状态',
    `owner_id`     VARCHAR(64) COMMENT '负责人id',
    `owner_name`   VARCHAR(64) COMMENT '负责人名称',
    `params`       LONGTEXT COMMENT '任务参数',
    `result`       LONGTEXT COMMENT '任务结果',
    `error_msg`    TEXT COMMENT '错误信息',
    PRIMARY KEY (`id`),
    UNIQUE KEY (`task_id`, `namespace`),
    INDEX `idx_distributed_task_gmt_expire` (`gmt_expire`)
) COMMENT '分布式任务管理';
CREATE TABLE naming_dictionary
(
    `id`                 BIGINT(20)  NOT NULL COMMENT '主键',
    `gmt_create`         DATETIME    NOT NULL COMMENT '创建时间',
    `gmt_modified`       DATETIME    NOT NULL COMMENT '修改时间',
    `uuid`               VARCHAR(64) NOT NULL COMMENT '全局唯一的uuid',
    `code`               VARCHAR(128) COMMENT 'code',
    `name`               VARCHAR(2048) COMMENT 'name',
    `extend_name`        VARCHAR(2048) COMMENT '扩展名称',
    `type`               TINYINT(4)  NOT NULL COMMENT '类型',
    `business_unit_uuid` VARCHAR(64) COMMENT '所属业务板块uuid',
    `tenant_id`          VARCHAR(64) COMMENT '租户id',
    `creator_id`         VARCHAR(128) COMMENT '创建人租户baseId',
    `creator_name`       VARCHAR(128) COMMENT '创建人显示名称',
    `modifier_id`        VARCHAR(128) COMMENT '修改人租户baseId',
    `modifier_name`      VARCHAR(128) COMMENT '修改人显示名称',
    `is_deleted`         TINYINT(4)  NOT NULL COMMENT '是否删除',
    PRIMARY KEY (`id`),
    UNIQUE KEY (`uuid`),
    INDEX `idx_unit_code` (`business_unit_uuid`, `code`)
) COMMENT '命名词典';
CREATE TABLE onedata_adjunct_word
(
    `adjunct_word_id`      INT(11)      NOT NULL COMMENT '修饰词的id',
    `biz_unit_id`          INT(11)      NOT NULL COMMENT '所属业务板块的id',
    `data_domain_id`       INT(11)      NOT NULL COMMENT '所属数据域的id',
    `adjunct_type_id`      INT(11)      NOT NULL COMMENT '修饰类型的id',
    `adjunct_word_name`    VARCHAR(32)  NOT NULL COMMENT '修饰词的英文名',
    `adjunct_word_name_cn` VARCHAR(128) NOT NULL COMMENT '修饰词的中文名',
    `adjunct_word_comment` TEXT COMMENT '修饰词的描述信息',
    `adjunct_word_creator` VARCHAR(256) COMMENT '修饰词的创建者',
    `adjunct_word_updater` VARCHAR(256) COMMENT '修饰词的最近更新者',
    `gmt_created`          DATETIME COMMENT '创建时间',
    `gmt_modified`         DATETIME COMMENT '最新更新时间',
    `status`               INT(11) COMMENT '状态字段，定义为1：草稿，2：待审核 3：审核通过，4.审核未通过，修饰词只有草稿和审核通过状态',
    PRIMARY KEY (`adjunct_word_id`),
    INDEX `idx_def_adjunct_word_biz_unit_id` (`biz_unit_id`),
    INDEX `idx_def_adjunct_word_data_domain_id` (`data_domain_id`),
    INDEX `idx_def_adjunct_word_adjunct_type_id` (`adjunct_type_id`),
    INDEX `idx_def_adjunct_word_adjunct_name_cn` (`adjunct_word_name_cn`)
) COMMENT 'onedata数据同步－修饰词';
CREATE TABLE onedata_atom_index
(
    `atom_index_id`        INT(11)      NOT NULL COMMENT '原子指标的id',
    `biz_unit_id`          INT(11)      NOT NULL COMMENT '所属业务板块的id',
    `data_domain_id`       INT(11)      NOT NULL COMMENT '所属数据域的id',
    `biz_process_id`       INT(11)      NOT NULL COMMENT '所属业务过程的id',
    `atom_index_name`      VARCHAR(128) NOT NULL COMMENT '原子指标英文名',
    `atom_index_name_cn`   VARCHAR(128) NOT NULL COMMENT '原子指标的中文名',
    `atom_index_comment`   TEXT COMMENT '原子指标的描述信息',
    `atom_index_data_type` VARCHAR(32)  NOT NULL COMMENT '原子指标的数据类型',
    `atom_index_unit`      VARCHAR(16) COMMENT '原子指标的单位',
    `atom_index_creator`   VARCHAR(256) COMMENT '原子指标的创建者',
    `atom_index_updater`   VARCHAR(256) COMMENT '原子指标的最近更新者',
    `gmt_created`          DATETIME COMMENT '创建时间',
    `gmt_modified`         DATETIME COMMENT '上次更新时间',
    `status`               INT(11) COMMENT '状态字段，定义为1：草稿，2：待审核 3：审核通过，4.审核未通过',
    `approve_opinion`      VARCHAR(1000) COMMENT '审批意见',
    `approver`             VARCHAR(16) COMMENT '最新一次的审核人',
    PRIMARY KEY (`atom_index_id`),
    INDEX `idx_def_atom_index_biz_unit_id` (`biz_unit_id`),
    INDEX `idx_def_atom_index_data_domain_id` (`data_domain_id`),
    INDEX `idx_def_atom_index_biz_process_id` (`biz_process_id`),
    INDEX `idx_def_atom_index_name_cn` (`atom_index_name_cn`)
) COMMENT 'onedata数据同步－原子指标';
CREATE TABLE onedata_biz_process
(
    `biz_process_id`      INT(11)      NOT NULL COMMENT '业务过程id',
    `biz_unit_id`         INT(11)      NOT NULL COMMENT '所属的业务板块的id',
    `data_domain_id`      INT(11)      NOT NULL COMMENT '所属数据域的id',
    `biz_process_name`    VARCHAR(32)  NOT NULL COMMENT '业务过程的英文名',
    `biz_process_name_cn` VARCHAR(128) NOT NULL COMMENT '业务过程的中文名',
    `biz_process_comment` TEXT COMMENT '业务过程的描述信息',
    `biz_process_creator` VARCHAR(16) COMMENT '业务过程的创建者',
    `biz_process_updater` VARCHAR(16) COMMENT '业务过程的最近修改者',
    `gmt_created`         DATETIME COMMENT '创建时间',
    `gmt_modified`        DATETIME COMMENT '最近一次修改时间',
    `status`              INT(11) COMMENT '状态字段，定义为1：草稿，2：待审核 3：审核通过，4.审核未通过',
    `approve_opinion`     VARCHAR(1000) COMMENT '审批意见',
    `approver`            VARCHAR(16) COMMENT '最新一次的审核人',
    PRIMARY KEY (`biz_process_id`),
    INDEX `idx_def_biz_process_biz_unit_id` (`biz_unit_id`),
    INDEX `idx_def_biz_process_data_domain_id` (`data_domain_id`)
) COMMENT 'onedata规范定义－业务过程';
CREATE TABLE onedata_data_domain
(
    `data_domain_id`              INT(11)      NOT NULL COMMENT '数据域的id',
    `biz_unit_id`                 INT(11)      NOT NULL COMMENT '所属业务板块的id',
    `data_domain_name`            VARCHAR(32)  NOT NULL COMMENT '数据域的英文名',
    `data_domain_name_cn`         VARCHAR(128) NOT NULL COMMENT '数据域的中文名',
    `data_domain_comment`         TEXT COMMENT '数据域的描述信息',
    `data_domain_owner`           VARCHAR(16) COMMENT '数据域的owner，即为数据域的创建者和审核人',
    `data_domain_owner_bak`       VARCHAR(16) COMMENT '数据域的备用owner',
    `gmt_created`                 DATETIME COMMENT '数据域的创建时间',
    `gmt_modified`                DATETIME COMMENT '数据域的最近修改时间',
    `status`                      INT(11) COMMENT '状态字段，定义为1：草稿，2：待审核 3：审核通过，4.审核未通过，数据域只有审核通过的状态',
    `approver`                    VARCHAR(16) COMMENT '最新一次的审核人',
    `data_domain_creator`         VARCHAR(16) COMMENT '数据域创建者',
    `data_domain_model_owner`     VARCHAR(64) COMMENT '模型设计的owner',
    `data_domain_model_owner_bak` VARCHAR(64) COMMENT '模型设计的owner',
    PRIMARY KEY (`data_domain_id`)
) COMMENT 'onedata规范定义－数据域';
CREATE TABLE onedata_derived_index
(
    `derived_index_id`             INT(11)      NOT NULL COMMENT '派生指标的id',
    `biz_unit_id`                  INT(11)      NOT NULL COMMENT '所属业务板块的id',
    `data_domain_id`               INT(11)      NOT NULL COMMENT '所属数据域的id',
    `biz_process_id`               INT(11)      NOT NULL COMMENT '所属业务过程的id',
    `atom_index_id`                INT(11)      NOT NULL COMMENT '基本的原子指标的id',
    `time_period_id`               INT(11)      NOT NULL COMMENT '派生指标使用的时间周期的id',
    `derived_index_name`           VARCHAR(128) NOT NULL COMMENT '派生指标的英文名',
    `derived_index_name_cn`        VARCHAR(128) NOT NULL COMMENT '派生指标的中文名',
    `derived_index_comment`        TEXT COMMENT '派生指标的描述信息',
    `derived_index_algorithm_desc` TEXT COMMENT '派生指标的算法说明',
    `derived_index_data_type`      VARCHAR(32) COMMENT '派生指标的数据类型',
    `derived_index_unit`           VARCHAR(16) COMMENT '派生指标的单位',
    `derived_index_creator`        VARCHAR(256) COMMENT '派生指标的创建者',
    `derived_index_updater`        VARCHAR(256) COMMENT '派生指标的最近更新者',
    `gmt_created`                  DATETIME COMMENT '创建时间',
    `gmt_modified`                 DATETIME COMMENT '最近更新时间',
    `status`                       INT(11) COMMENT '状态字段，定义为1：草稿，2：待审核 3：审核通过，4.审核未通过',
    `approve_opinion`              VARCHAR(1000) COMMENT '审批意见',
    `approver`                     VARCHAR(16) COMMENT '最新一次的审核人',
    PRIMARY KEY (`derived_index_id`),
    INDEX `idx_def_derived_index_biz_unit_id` (`biz_unit_id`),
    INDEX `idx_def_derived_index_biz_process_id` (`biz_process_id`),
    INDEX `idx_def_derived_index_time_period_id` (`time_period_id`),
    INDEX `idx_def_derived_index_suffix_index` (`time_period_id`, `biz_unit_id`, `atom_index_id`)
) COMMENT 'onedata规范定义－派生指标';
CREATE TABLE onedata_derived_index_adjunct_word_relation
(
    `id`               INT(11) NOT NULL COMMENT '自增id',
    `derived_index_id` INT(11) NOT NULL COMMENT '派生指标id',
    `adjunct_word_id`  INT(11) NOT NULL COMMENT '修饰词id',
    PRIMARY KEY (`id`),
    UNIQUE KEY (`derived_index_id`, `adjunct_word_id`),
    INDEX `idx_def_derived_index_adjunct_word_relation_adjunct_word_id` (`adjunct_word_id`)
) COMMENT 'onedata规范定义－修饰词和派生指标关系表';
CREATE TABLE onedata_dimension
(
    `dimension_id`      INT(11)      NOT NULL COMMENT '维度的id',
    `biz_unit_id`       INT(11)      NOT NULL COMMENT '所属业务板块的id',
    `data_domain_id`    INT(11)      NOT NULL COMMENT '所属数据域的id',
    `dimension_name`    VARCHAR(32)  NOT NULL COMMENT '维度名',
    `dimension_name_cn` VARCHAR(128) NOT NULL COMMENT '维度中文名',
    `dimension_comment` TEXT COMMENT '维度的描述信息',
    `dimension_creator` VARCHAR(16) COMMENT '维度的创建者',
    `dimension_updater` VARCHAR(16) COMMENT '维度的上次更新者',
    `gmt_created`       DATETIME COMMENT '创建时间',
    `gmt_modified`      DATETIME COMMENT '上次更新时间',
    `status`            INT(11) COMMENT '状态字段，定义为1：草稿，2：待审核 3：审核通过，4.审核未通过',
    `approve_opinion`   VARCHAR(1000) COMMENT '审批意见',
    `approver`          VARCHAR(16) COMMENT '最新一次的审核人',
    PRIMARY KEY (`dimension_id`),
    INDEX `idx_def_dimension_biz_unit_id` (`biz_unit_id`),
    INDEX `idx_def_dimension_data_domain_id` (`data_domain_id`)
) COMMENT 'onedata规范定义－维度';
CREATE TABLE onedata_dimension_attr
(
    `dimension_attr_id`        INT(11)      NOT NULL COMMENT '维度属性的id',
    `biz_unit_id`              INT(11)      NOT NULL COMMENT '所属业务板块的id',
    `data_domain_id`           INT(11)      NOT NULL COMMENT '所属数据域的id',
    `dimension_id`             INT(11)      NOT NULL COMMENT '所属的维度id',
    `dimension_attr_name`      VARCHAR(128) NOT NULL COMMENT '维度属性的英文名',
    `dimension_attr_name_cn`   VARCHAR(128) NOT NULL COMMENT '维度属性的中文名',
    `dimension_attr_comment`   TEXT COMMENT '维度属性的描述',
    `dimension_attr_data_type` VARCHAR(32)  NOT NULL COMMENT '维度属性的数据类型',
    `dimension_attr_creator`   VARCHAR(256) COMMENT '维度属性的创建者',
    `dimension_attr_updater`   VARCHAR(256) COMMENT '维度属性的上次更新者',
    `gmt_created`              DATETIME COMMENT '创建时间',
    `gmt_modified`             DATETIME COMMENT '更新时间',
    `status`                   INT(11) COMMENT '状态字段，定义为1：草稿，2：待审核 3：审核通过，4.审核未通过',
    `approve_opinion`          VARCHAR(1000) COMMENT '审批意见',
    `approver`                 VARCHAR(16) COMMENT '最新一次的审核人',
    `dimension_attr_isindex`   VARCHAR(8) COMMENT '是否是主键，默认为0不是，1为是',
    PRIMARY KEY (`dimension_attr_id`),
    INDEX `idx_def_dimension_attr_biz_unit_id` (`biz_unit_id`),
    INDEX `idx_def_dimension_attr_data_domain_id` (`data_domain_id`),
    INDEX `idx_def_dimension_attr_dimension_id` (`dimension_id`)
) COMMENT 'onedata规范定义－维度属性';
CREATE TABLE onedata_time_period
(
    `time_period_id`      INT(11)      NOT NULL COMMENT '时间周期的id',
    `biz_unit_id`         INT(11)      NOT NULL COMMENT '所属的业务板块的id',
    `time_period_name`    VARCHAR(32)  NOT NULL COMMENT '时间周期的英文名',
    `time_period_name_cn` VARCHAR(128) NOT NULL COMMENT '时间周期的中文名',
    `time_period_comment` TEXT COMMENT '时间周期的描述',
    `time_period_creator` VARCHAR(256) COMMENT '时间周期的创建者',
    `time_period_updater` VARCHAR(256) COMMENT '时间周期的修改者',
    `gmt_created`         DATETIME COMMENT '创建时间',
    `gmt_modified`        DATETIME COMMENT '修改时间',
    `status`              INT(11) COMMENT '状态字段，定义为1：草稿，2：待审核 3：审核通过，4.审核未通过',
    `approve_opinion`     VARCHAR(1000) COMMENT '审核意见',
    `approver`            VARCHAR(16) COMMENT '审核人',
    PRIMARY KEY (`time_period_id`),
    INDEX `idx_def_time_period_biz_unit_id` (`biz_unit_id`),
    INDEX `idx_def_time_period_name_cn` (`time_period_name_cn`)
) COMMENT 'onedata数据同步表-时间周期';
CREATE TABLE se_activity_instance
(
    `id`                                BIGINT(20)   NOT NULL COMMENT 'PK',
    `gmt_create`                        DATETIME     NOT NULL COMMENT 'create time',
    `gmt_modified`                      DATETIME     NOT NULL COMMENT 'modification time',
    `process_instance_id`               BIGINT(20) COMMENT 'process instance id',
    `process_definition_id_and_version` VARCHAR(255) NOT NULL COMMENT 'process definition id and version',
    `process_definition_activity_id`    VARCHAR(64)  NOT NULL COMMENT 'process definition activity id',
    PRIMARY KEY (`id`),
    INDEX `idx_process_instance_id` (`process_instance_id`)
) COMMENT 'smartengine';
CREATE TABLE se_default_id_generator
(
    `id`           BIGINT(20) NOT NULL COMMENT '主键',
    `gmt_create`   DATETIME   NOT NULL COMMENT '创建时间',
    `gmt_modified` DATETIME   NOT NULL COMMENT '修改时间',
    PRIMARY KEY (`id`)
) COMMENT 'smartengine主键生成器';
CREATE TABLE se_deployment_instance
(
    `id`                         BIGINT(20)   NOT NULL COMMENT 'PK',
    `gmt_create`                 DATETIME     NOT NULL COMMENT 'create time',
    `gmt_modified`               DATETIME     NOT NULL COMMENT 'modification time',
    `process_definition_id`      VARCHAR(255) NOT NULL COMMENT 'process definition id',
    `process_definition_version` VARCHAR(255) COMMENT 'process definition version',
    `process_definition_type`    VARCHAR(255) COMMENT 'process definition type',
    `process_definition_code`    VARCHAR(255) COMMENT 'process definition code',
    `process_definition_name`    VARCHAR(255) COMMENT 'process definition name',
    `process_definition_desc`    VARCHAR(255) COMMENT 'process definition desc',
    `process_definition_content` MEDIUMTEXT   NOT NULL COMMENT 'process definition content',
    `deployment_user_id`         VARCHAR(128) NOT NULL COMMENT 'deployment user id',
    `deployment_status`          VARCHAR(64)  NOT NULL COMMENT 'deployment status',
    `logic_status`               VARCHAR(64)  NOT NULL COMMENT 'logic status',
    PRIMARY KEY (`id`),
    INDEX `idx_user_id_and_logic_status` (`logic_status`, `deployment_user_id`)
) COMMENT 'smartengine';
CREATE TABLE se_execution_instance
(
    `id`                                BIGINT(20)   NOT NULL COMMENT 'PK',
    `gmt_create`                        DATETIME     NOT NULL COMMENT 'create time',
    `gmt_modified`                      DATETIME     NOT NULL COMMENT 'modification time',
    `process_instance_id`               BIGINT(20)   NOT NULL COMMENT 'process instance id',
    `process_definition_id_and_version` VARCHAR(255) NOT NULL COMMENT 'process definition id and version',
    `process_definition_activity_id`    VARCHAR(255) NOT NULL COMMENT 'process definition activity id',
    `activity_instance_id`              BIGINT(20)   NOT NULL COMMENT 'activity instance id',
    `active`                            TINYINT(4)   NOT NULL COMMENT '1:active 0:inactive',
    PRIMARY KEY (`id`),
    INDEX `idx_process_instance_id_and_status` (`process_instance_id`, `active`),
    INDEX `idx_process_instance_id_and_activity_instance_id` (`process_instance_id`, `activity_instance_id`)
) COMMENT 'smartengine';
CREATE TABLE se_process_instance
(
    `id`                                BIGINT(20)   NOT NULL COMMENT 'PK',
    `gmt_create`                        DATETIME     NOT NULL COMMENT 'create time',
    `gmt_modified`                      DATETIME     NOT NULL COMMENT 'modification time',
    `process_definition_id_and_version` VARCHAR(128) NOT NULL COMMENT 'process definition id and version',
    `process_definition_type`           VARCHAR(255) COMMENT 'process definition type',
    `status`                            VARCHAR(64)  NOT NULL COMMENT '1.running 2.completed 3.aborted',
    `parent_process_instance_id`        BIGINT(20) COMMENT 'parent process instance id',
    `parent_execution_instance_id`      BIGINT(20) COMMENT 'parent execution instance id',
    `start_user_id`                     VARCHAR(128) COMMENT 'start user id',
    `biz_unique_id`                     VARCHAR(255) COMMENT 'biz unique id',
    `reason`                            VARCHAR(255) COMMENT 'reason',
    `comment`                           VARCHAR(255) COMMENT 'comment',
    `title`                             VARCHAR(255) COMMENT 'title',
    `tag`                               VARCHAR(255) COMMENT 'tag',
    PRIMARY KEY (`id`),
    INDEX `idx_start_user_id` (`start_user_id`),
    INDEX `idx_status` (`status`)
) COMMENT 'smartengine';
CREATE TABLE se_task_assignee_instance
(
    `id`                  BIGINT(20)   NOT NULL COMMENT 'PK',
    `gmt_create`          DATETIME     NOT NULL COMMENT 'create time',
    `gmt_modified`        DATETIME     NOT NULL COMMENT 'modification time',
    `process_instance_id` BIGINT(20)   NOT NULL COMMENT 'process instance id',
    `task_instance_id`    BIGINT(20)   NOT NULL COMMENT 'task instance id',
    `assignee_id`         VARCHAR(255) NOT NULL COMMENT 'assignee id',
    `assignee_type`       VARCHAR(128) NOT NULL COMMENT 'assignee type',
    PRIMARY KEY (`id`),
    INDEX `idx_task_instance_id` (`task_instance_id`),
    INDEX `idx_assignee_id_and_type` (`assignee_id`, `assignee_type`)
) COMMENT 'smartengine';
CREATE TABLE se_task_instance
(
    `id`                                BIGINT(20)   NOT NULL COMMENT 'PK',
    `gmt_create`                        DATETIME     NOT NULL COMMENT 'create time',
    `gmt_modified`                      DATETIME     NOT NULL COMMENT 'modification time',
    `process_instance_id`               BIGINT(20)   NOT NULL COMMENT 'process instance id',
    `process_definition_id_and_version` VARCHAR(128) COMMENT 'process definition id and version',
    `process_definition_type`           VARCHAR(255) COMMENT 'process definition type',
    `activity_instance_id`              BIGINT(20)   NOT NULL COMMENT 'activity instance id',
    `process_definition_activity_id`    VARCHAR(255) NOT NULL COMMENT 'process definition activity id',
    `execution_instance_id`             BIGINT(20)   NOT NULL COMMENT 'execution instance id',
    `claim_user_id`                     VARCHAR(255) COMMENT 'claim user id',
    `title`                             VARCHAR(255) COMMENT 'title',
    `priority`                          INT(11) COMMENT 'priority',
    `tag`                               VARCHAR(255) COMMENT 'tag',
    `claim_time`                        DATETIME COMMENT 'claim time',
    `complete_time`                     DATETIME COMMENT 'complete time',
    `status`                            VARCHAR(255) NOT NULL COMMENT 'status',
    `comment`                           VARCHAR(255) COMMENT 'comment',
    `extension`                         VARCHAR(255) COMMENT 'extension',
    PRIMARY KEY (`id`),
    INDEX `idx_status` (`status`),
    INDEX `idx_process_instance_id_and_status` (`process_instance_id`, `status`),
    INDEX `idx_process_definition_type` (`process_definition_type`),
    INDEX `idx_process_instance_id` (`process_instance_id`),
    INDEX `idx_claim_user_id` (`claim_user_id`),
    INDEX `idx_tag` (`tag`),
    INDEX `idx_activity_instance_id` (`activity_instance_id`),
    INDEX `idx_process_definition_activity_id` (`process_definition_activity_id`)
) COMMENT 'smartengine';
CREATE TABLE se_variable_instance
(
    `id`                    BIGINT(20)   NOT NULL COMMENT 'PK',
    `gmt_create`            DATETIME     NOT NULL COMMENT 'create time',
    `gmt_modified`          DATETIME     NOT NULL COMMENT 'modification time',
    `process_instance_id`   BIGINT(20)   NOT NULL COMMENT 'process instance id',
    `execution_instance_id` BIGINT(20) COMMENT 'execution instance id',
    `field_key`             VARCHAR(128) NOT NULL COMMENT 'field key',
    `field_type`            VARCHAR(128) NOT NULL COMMENT 'field type',
    `field_double_value`    DECIMAL COMMENT 'field double value',
    `field_long_value`      BIGINT(20) COMMENT 'field long value',
    `field_string_value`    VARCHAR(4000) COMMENT 'field string value',
    PRIMARY KEY (`id`),
    INDEX `idx_process_instance_id_and_execution_instance_id` (`process_instance_id`, `execution_instance_id`)
) COMMENT 'smartengine';
CREATE TABLE table_source
(
    `id`                         BIGINT(20)   NOT NULL COMMENT '主键',
    `gmt_create`                 DATETIME     NOT NULL COMMENT '创建时间',
    `gmt_modified`               DATETIME     NOT NULL COMMENT '修改时间',
    `table_uuid`                 VARCHAR(64)  NOT NULL COMMENT '表uuid',
    `column_uuid`                VARCHAR(64) COMMENT '表字段uuid',
    `source_project_name`        VARCHAR(128) NOT NULL COMMENT '来源项目',
    `source_table_code`          VARCHAR(128) NOT NULL COMMENT '来源表编码',
    `source_column_code`         VARCHAR(128) COMMENT '来源字段编码',
    `table_source_relation_uuid` VARCHAR(64) COMMENT '关联来源关系UUID',
    `business_unit_uuid`         VARCHAR(64)  NOT NULL COMMENT '业务板块uuid',
    `tenant_id`                  VARCHAR(64)  NOT NULL COMMENT '租户ID',
    `creator_id`                 VARCHAR(128) NOT NULL COMMENT '创建人baseID',
    `creator_name`               VARCHAR(128) NOT NULL COMMENT '创建人名称',
    `modifier_id`                VARCHAR(128) NOT NULL COMMENT '修改人baseID',
    `modifier_name`              VARCHAR(128) NOT NULL COMMENT '修改人名称',
    `is_deleted`                 TINYINT(4)   NOT NULL COMMENT '是否删除',
    PRIMARY KEY (`id`)
) COMMENT '表来源表';
CREATE TABLE table_source_relation
(
    `id`                        BIGINT(20)   NOT NULL COMMENT '主键',
    `gmt_create`                DATETIME     NOT NULL COMMENT '创建时间',
    `gmt_modified`              DATETIME     NOT NULL COMMENT '修改时间',
    `uuid`                      VARCHAR(64)  NOT NULL COMMENT '来源关系唯一标识',
    `table_uuid`                VARCHAR(64)  NOT NULL COMMENT '表uuid',
    `column_uuid`               VARCHAR(64) COMMENT '字段uuid',
    `right_source_project_name` VARCHAR(128) COMMENT '右表项目',
    `right_source_table_code`   VARCHAR(128) COMMENT '右表编码',
    `right_source_column_code`  VARCHAR(128) COMMENT '右表字段编码',
    `creator_id`                VARCHAR(128) NOT NULL COMMENT '创建人baseID',
    `creator_name`              VARCHAR(128) NOT NULL COMMENT '创建人名称',
    `modifier_id`               VARCHAR(128) NOT NULL COMMENT '修改人baseID',
    `modifier_name`             VARCHAR(128) NOT NULL COMMENT '修改人名称',
    `is_deleted`                TINYINT(4)   NOT NULL COMMENT '是否删除',
    `business_unit_uuid`        VARCHAR(64)  NOT NULL COMMENT '业务板块uuid',
    `tenant_id`                 VARCHAR(64)  NOT NULL COMMENT '租户ID',
    PRIMARY KEY (`id`)
) COMMENT '表来源关系表';
CREATE TABLE tag
(
    `id`                 BIGINT(20) NOT NULL COMMENT '主键',
    `gmt_create`         DATETIME   NOT NULL COMMENT '创建时间',
    `gmt_modified`       DATETIME   NOT NULL COMMENT '修改时间',
    `tenant_id`          VARCHAR(128) COMMENT '租户id',
    `creator_id`         VARCHAR(64) COMMENT '创建人租户baseId',
    `creator_name`       VARCHAR(64) COMMENT '创建人显示名称',
    `uuid`               VARCHAR(128) COMMENT '全局唯一的uuid',
    `business_unit_uuid` VARCHAR(64) COMMENT '业务板块uuid',
    `name`               VARCHAR(64) COMMENT '标签名称',
    `tag_type`           TINYINT(4) COMMENT '标签类型',
    `source_type`        TINYINT(4) COMMENT '来源类型',
    PRIMARY KEY (`id`),
    UNIQUE KEY (`uuid`),
    UNIQUE KEY (`tenant_id`, `business_unit_uuid`, `tag_type`, `name`)
) COMMENT '通用标签表';
CREATE TABLE tag_rel_entity
(
    `id`                 BIGINT(20) NOT NULL COMMENT '主键',
    `gmt_create`         DATETIME   NOT NULL COMMENT '创建时间',
    `gmt_modified`       DATETIME   NOT NULL COMMENT '修改时间',
    `tenant_id`          VARCHAR(128) COMMENT '租户id',
    `creator_id`         VARCHAR(64) COMMENT '创建人租户baseId',
    `creator_name`       VARCHAR(64) COMMENT '创建人显示名称',
    `business_unit_uuid` VARCHAR(64) COMMENT '业务板块uuid',
    `tag_uuid`           VARCHAR(64) COMMENT '标签uuid',
    `entity_uuid`        VARCHAR(64) COMMENT '实体uuid',
    PRIMARY KEY (`id`),
    INDEX `idx_tag_rel_entity_uuid` (`entity_uuid`)
) COMMENT '标签和实体的关联关系';
CREATE TABLE task
(
    `id`                BIGINT(20)    NOT NULL COMMENT '主键',
    `gmt_create`        DATETIME      NOT NULL COMMENT '创建时间',
    `gmt_modified`      DATETIME      NOT NULL COMMENT '修改时间',
    `tenant_id`         VARCHAR(64)   NOT NULL COMMENT '租户id',
    `project_id`        VARCHAR(64)   NOT NULL COMMENT '当前项目空间id',
    `source_project_id` VARCHAR(64)   NOT NULL COMMENT '来源项目空间id',
    `creator_id`        VARCHAR(128)  NOT NULL COMMENT '创建者id',
    `creator_name`      VARCHAR(128)  NOT NULL COMMENT '创建者名称',
    `description`       VARCHAR(2048) COMMENT '描述',
    `is_deleted`        TINYINT(4)    NOT NULL COMMENT '是否删除',
    `uuid`              VARCHAR(64)   NOT NULL COMMENT '任务编码',
    `type`              VARCHAR(32)   NOT NULL COMMENT '前缀方式：PREFIX,精确表名：CODE',
    `content`           TEXT          NOT NULL COMMENT '内容，表名或者前缀列表',
    `pattern`           VARCHAR(4096) NOT NULL COMMENT '解析规则',
    `reverse_mode`      VARCHAR(32)   NOT NULL COMMENT '全量：DELETE_BEFORE_INSERT \n增量：INSERT_IF_NOT_EXIST',
    `status`            VARCHAR(32)   NOT NULL COMMENT '未开始：CREATED\n进行中：RUNNING\n完成：FINISHED',
    `name`              VARCHAR(128)  NOT NULL COMMENT '任务名称',
    `total`             INT(10) COMMENT '任务明细数量',
    PRIMARY KEY (`id`)
) COMMENT '逆向任务表';
CREATE TABLE task_detail
(
    `id`            BIGINT(20)   NOT NULL COMMENT '主键',
    `gmt_create`    DATETIME     NOT NULL COMMENT '创建时间',
    `gmt_modified`  DATETIME     NOT NULL COMMENT '修改时间',
    `command`       TEXT         NOT NULL COMMENT '冗余的执行参数',
    `dm_context`    VARCHAR(2048) COMMENT '模型上下文',
    `type`          TINYINT(4)   NOT NULL COMMENT '模型类型，维度表、事实表、汇总表',
    `table_meta`    TEXT COMMENT '表元仓数据',
    `result`        TEXT COMMENT '执行结果',
    `status`        VARCHAR(32)  NOT NULL COMMENT '任务明细执行状态',
    `task_id`       VARCHAR(64)  NOT NULL COMMENT '关联的任务uuid',
    `modifier_id`   VARCHAR(128) NOT NULL COMMENT '修改人id',
    `modifier_name` VARCHAR(128) NOT NULL COMMENT '修改人名称',
    `is_deleted`    TINYINT(4)   NOT NULL COMMENT '是否删除',
    `uuid`          VARCHAR(64)  NOT NULL COMMENT '主键',
    PRIMARY KEY (`id`)
) COMMENT '任务明细表';
CREATE TABLE t_deploy_process
(
    `id`                      BIGINT(20)   NOT NULL COMMENT '主键',
    `gmt_create`              DATETIME     NOT NULL COMMENT '创建时间',
    `gmt_modified`            DATETIME COMMENT '修改时间',
    `commit_object_id`        VARCHAR(128) NOT NULL COMMENT '提交物id',
    `uuid`                    VARCHAR(128) NOT NULL COMMENT 'uuid',
    `model_id`                VARCHAR(128) NOT NULL COMMENT '模型id',
    `model_type`              VARCHAR(64)  NOT NULL COMMENT '模型类型',
    `agree_process_id`        VARCHAR(128) COMMENT '审批流id',
    `workflow_process_id`     VARCHAR(128) NOT NULL COMMENT '工作流id',
    `big_engine_type`         VARCHAR(64)  NOT NULL COMMENT '部署类型',
    `big_engine_ds`           VARCHAR(128) NOT NULL COMMENT '数据源',
    `release_runtime_env`     VARCHAR(64)  NOT NULL COMMENT '部署环境',
    `status`                  VARCHAR(64)  NOT NULL COMMENT '流程状态',
    `business_unit_uuid`      VARCHAR(128) NOT NULL COMMENT '业务板块uuid',
    `business_process_name`   VARCHAR(128) COMMENT '业务过程code',
    `model_code`              VARCHAR(128) COMMENT '模型code',
    `tenant_id`               VARCHAR(64)  NOT NULL COMMENT '租户id',
    `project_id`              VARCHAR(64)  NOT NULL COMMENT '项目id',
    `releaser_id`             VARCHAR(64)  NOT NULL COMMENT '发布者id',
    `releaser_name`           VARCHAR(64)  NOT NULL COMMENT '发布者名称',
    `release_date`            DATETIME     NOT NULL COMMENT '发布时间',
    `release_remark`          TEXT         NOT NULL COMMENT '发布备注',
    `creator_id`              VARCHAR(128) NOT NULL COMMENT '创建人租户baseId',
    `creator_name`            VARCHAR(128) NOT NULL COMMENT '创建人显示名称',
    `modifier_id`             VARCHAR(128) COMMENT '修改人租户baseid',
    `modifier_name`           VARCHAR(128) COMMENT '修改人显示名称',
    `version`                 BIGINT(20)   NOT NULL COMMENT '版本号',
    `big_engine_dialect_type` VARCHAR(64)  NOT NULL COMMENT '大数据dsl方言类型',
    `resource_group_id`       VARCHAR(128) COMMENT '调度独立资源组id',
    `deploy_mode`             VARCHAR(64) COMMENT '部署模式',
    PRIMARY KEY (`id`)
) COMMENT '部署过程表';
CREATE TABLE t_draft_object
(
    `id`                       BIGINT(20)   NOT NULL COMMENT '主键',
    `gmt_create`               DATETIME     NOT NULL COMMENT '创建时间',
    `gmt_modified`             DATETIME COMMENT '修改时间',
    `committer_id`             VARCHAR(64)  NOT NULL COMMENT '提交人id',
    `committer_name`           VARCHAR(64) COMMENT '提交人姓名',
    `commit_remark`            TEXT         NOT NULL COMMENT '提交备注',
    `fml_statement`            TEXT         NOT NULL COMMENT 'fml语句',
    `business_unit_uuid`       VARCHAR(64)  NOT NULL COMMENT '所属的业务板块uuid',
    `tenant_id`                VARCHAR(128) NOT NULL COMMENT '租户id',
    `project_id`               VARCHAR(128) NOT NULL COMMENT '项目id',
    `creator_id`               VARCHAR(64)  NOT NULL COMMENT '创建人租户baseId',
    `creator_name`             VARCHAR(64) COMMENT '创建人显示名称',
    `modifier_id`              VARCHAR(64) COMMENT '修改人租户baseId',
    `modifier_name`            VARCHAR(64) COMMENT '修改人显示名称',
    `version`                  INT(11)      NOT NULL COMMENT '版本号',
    `commit_date`              TIMESTAMP    NULL COMMENT '提交时间',
    `model_id`                 VARCHAR(128) NOT NULL COMMENT '模型id',
    `model_type`               VARCHAR(128) NOT NULL COMMENT '模型类型',
    `business_process_name`    VARCHAR(70) COMMENT '业务过程名称',
    `uuid`                     VARCHAR(128) NOT NULL COMMENT '提交对象唯一id，替代id用于迁移方便，符合团队规范',
    `previous_draft_object_id` VARCHAR(128) COMMENT '上一次保存草稿对象的uuid',
    `model_version`            INT(11)      NOT NULL COMMENT '模型版本',
    `model_code`               VARCHAR(128) COMMENT '模型code',
    PRIMARY KEY (`id`),
    UNIQUE KEY (`uuid`),
    INDEX `idx_model` (`model_id`)
) COMMENT '草稿对象表';
CREATE TABLE t_stage_object
(
    `id`                    BIGINT(20)   NOT NULL COMMENT '主键',
    `gmt_create`            DATETIME     NOT NULL COMMENT '创建时间',
    `gmt_modified`          DATETIME COMMENT '修改时间',
    `uuid`                  VARCHAR(128) NOT NULL COMMENT '暂存对象唯一id，替代id用于迁移方便，符合团队规范',
    `model_id`              VARCHAR(128) NOT NULL COMMENT '模型id',
    `model_type`            VARCHAR(128) NOT NULL COMMENT '模型类型',
    `business_process_name` VARCHAR(70) COMMENT '业务过程名称',
    `commit_object_id`      VARCHAR(128) NOT NULL COMMENT '提交物对象id',
    `model_code`            VARCHAR(128) COMMENT '模型code',
    `project_id`            VARCHAR(128) COMMENT '项目id',
    PRIMARY KEY (`id`),
    UNIQUE KEY (`uuid`),
    INDEX `idx_model` (`model_id`, `model_type`)
) COMMENT '暂存对象表';
