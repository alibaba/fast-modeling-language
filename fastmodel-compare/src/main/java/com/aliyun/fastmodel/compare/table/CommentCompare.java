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

package com.aliyun.fastmodel.compare.table;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/8/30
 */
public class CommentCompare implements TableElementCompare {
    @Override
    public List<BaseStatement> compareTableElement(CreateTable before, CreateTable after) {
        Comment beforeComment = before.getComment();
        Comment afterComment = after.getComment();
        if (isChangeComment(beforeComment, afterComment)) {
            Comment setComment = (afterComment == null || afterComment.getComment() == null) ? new Comment(
                StringUtils.EMPTY) : afterComment;
            return ImmutableList.of(new SetTableComment(after.getQualifiedName(), setComment));
        }
        return ImmutableList.of();
    }

    private boolean isChangeComment(Comment comment, Comment afterComment) {
        if (comment == null && afterComment == null) {
            return false;
        }
        if (comment == null && afterComment != null) {
            return StringUtils.isNotBlank(afterComment.getComment());
        }
        if (comment != null && afterComment == null) {
            return StringUtils.isNotBlank(comment.getComment());
        }
        return !StringUtils.equals(StringUtils.trimToEmpty(comment.getComment()),
            StringUtils.trimToEmpty(afterComment.getComment()));
    }

}
