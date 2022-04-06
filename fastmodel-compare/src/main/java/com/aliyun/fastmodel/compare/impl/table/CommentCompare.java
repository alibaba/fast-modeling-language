/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.impl.table;

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
