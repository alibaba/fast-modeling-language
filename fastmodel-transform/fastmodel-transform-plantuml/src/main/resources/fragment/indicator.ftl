entity ${indicatorName} <<(I , #FF7700) <#if comment??>{comment}</#if>$>> {
<#if expr??>
    ${expr} : ${dataType}
</#if>
}

