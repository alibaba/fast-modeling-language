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

package com.aliyun.fastmodel.core.tree.statement;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import lombok.Getter;

/**
 * 表示创建的语句内容
 *
 * @author panguanjing
 * @date 2020/9/2
 */
@Getter
public abstract class BaseCreate extends BaseOperatorStatement {

    protected CreateElement createElement;

    public BaseCreate(CreateElement createElement) {
        super(createElement.getQualifiedName());
        this.createElement = createElement;
    }

    public String getCommentValue() {
        Comment comment = createElement.getComment();
        return comment != null ? comment.getComment() : null;
    }

    public String getAliasedNameValue() {
        if (getAliasedName() != null) {
            return getAliasedName().getName();
        }
        return null;
    }

    public boolean isPropertyEmpty() {
        List<Property> properties = createElement.getProperties();
        return properties == null || properties.isEmpty();
    }

    public Comment getComment() {return getCreateElement().getComment();}

    public List<Property> getProperties() {return getCreateElement().getProperties();}

    public boolean isNotExists() {return getCreateElement().isNotExists();}

    public AliasedName getAliasedName() {return getCreateElement().getAliasedName();}

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBaseCreate(this, context);
    }
}
