------------------------------------------------------------
-- 字符串函数
-- 由脚本引擎预先定义，如果有性能问题，可用C++改写
-- @author jiangchanghao
-- @since 2012-10-11
-----------------------------------------------------------
function lowerstr(str)
    return string.lower(str)
end

function upperstr(str)
    return string.upper(str)
end

function strlen(str)
    return string.len(str)
end

function strleft(str, n)
    return string.sub(str, 1, n)
end

function strright(str, n)
    if (n < string.len(str))
    then    
        return string.sub(str, string.len(str)-n+1, -1)
    else
        return str
    end
end

function strmid(str, i,j)
    if (i <= 0)
    then return ""
    end
    return string.sub(str, i,j+i-1)
end

function ltrim(str)
    s,_ = string.gsub(str,'^ +','')
    return s
end

function rtrim(str)
    s, _ = string.gsub(str,' +$','')
    return s
end

function numtostr(num, N)
    return string.format(string.format('%s.%if','%',N),num)
end

function strcat(des,str)
    return string.format('%s%s', des, str)
end

function strtonum(str)
    return tonumber(str)
end

function strtonumex(str, default)
    local num = tonumber(str)
    if isvalid(num) then
        return num
    else
        return default
    end
end

function strinsert(str, index, str1)
    return string.format('%s%s%s',string.sub(str,1,index),str1,string.sub(str,index+1,-1))
end

function strremove(str, index, cound)
    return string.format('%s%s',string.sub(str,1,index),string.sub(str,index+cound+1,-1))
end

function strfind(str, s1, n, timetag)
    i,_ = string.find(str,s1,n)
    if (i == nil)
        then return 0
        else return i
    end
end

function strreplace(str, strold, strnew)
    s, _ = string.gsub(str, strold, strnew)
    return s
end

function strtrimleft(str, str1)
    s, _ = string.gsub(str,string.format('%s%s%s','^',str1,'+'),'')
    return s
end

function strtrimright(str, str1)
    s, _ = string.gsub(str,string.format('%s%s',str1,'+$'),'')
    return s
end

function strcmp(str1,str2)
    if (str1 < str2)
    then return -1
    else if (str1 == str2)
        then return 0
        else return 1
        end
    end
end

function stricmp(str1,str2)
    s1 = string.upper(str1)
    s2 = string.upper(str2)
    if (s1 < s2)
    then return -1
    else if (s1 == s2)
        then return 0
        else return 1
        end
    end
end

function strncmp(str1,str2,n)
    s1 = string.sub(str1,1,n)
    s2 = string.sub(str2,1,n)
    if (s1 < s2)
    then return -1
    else if (s1 == s2)
        then return 0
        else return 1
        end
    end
end

function fmt(...)
    local tt = {...}
    local len = #tt
    if (len == 1) then
        return tostring(tt[1])
    end

    fc = FmtCache()
    fc:setFmtString(tostring(tt[1]))
    local i = 2
    repeat
        local v = tt[i]
        if (type(v) == "number") then 
            fc:pushDouble(i, v)
        elseif (type(v) == "string") then
            fc:pushString(i, v)
        elseif (type(v) == "boolean") then
            if (v) then
                fc:pushDouble(i, 1)
            else
                fc:pushDouble(i, 0)
            end
        else
            fc:pushString(i, "nil")
        end
        i = i + 1
    until i > len
    return fc:getFmtString()
end

