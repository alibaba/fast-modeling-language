Table(${tableCode}<#if comment??>,${comment}</#if>) {
<#if cols??>
<#list cols as col>
Column(<#if col.primaryKey??>PK("${col.colName}")<#else>"${col.colName}"</#if>, "${col.colType}", <#if col.notNull??>1<#else>0</#if>,"", <#if col.colAlias??>"${col.colAlias}"<#else>""</#if>, <#if col.colComment??>"${col.colComment}"<#else>""</#if>)
</#list>
</#if>
}
<#if constraints??>
<#list constraints as constraint>
${constraint.left.table} --> ${constraint.right.table}
</#list>
</#if>
