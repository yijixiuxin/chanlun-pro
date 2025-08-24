-------------------------------------------------------------
-- 脚本引擎工作期间用到的辅助函数
-- @author zhangjin
-- @since 2012-7-17
-------------------------------------------------------------


function unpackEx(arglist)
    local arg = {}
    for k, v in pairs(arglist) do
        print(k, v)
        table.insert(arg, v)
    end
    return unpack(arg)
end

function pack(func, arglist)
    f = _G[func]
    --print(func, type(f))
    return f(unpack(arglist))
end

List = {}
function List.new ()
    return {first = 0, last = -1}
end

function List.pushleft (list, value)
    local first = list.first - 1
    list.first = first
    list[first] = value
end

function List.pushright (list, value)
    local last = list.last + 1
    list.last = last
    list[last] = value
end

function List.popright (list)
    local last = list.last
    if list.first > last then error("list is empty") end
    local value = list[last]
    list[last] = nil -- to allow garbage collection
    list.last = last - 1
    return value
end

function List.popleft(list)
    local first = list.first
    local value = list[first]
    list[first] = nil
    list.first = first + 1
    return value
end

function isValid(v)
    if type(v) == "number" then
        return v == v and -1.7*10^308 < v and v < 1.7*10^308
    else
        if type(v) == "nil" then
            return false
        else
            return true
        end
    end
end

function isvalid(v)
    return isValid(v)
end

function d2b(v)
    return v ~= 0 --此处应该是 double值为正的时候返回true，还是double不为0的时候返回true
end

function b2d(v)
    if v then
        return 1
    else
        return 0
    end
end

FLOAT_ERROR = 1e-6;
function isZero(value)
    return math.abs(value) <= FLOAT_ERROR;
end

function isGreaterThanZero(value)
    return value > FLOAT_ERROR
end

function isLessThanZero(value)
    return value < -1 * FLOAT_ERROR
end    

function isequalv(left, right)
    if type(left)=='string' and type(right)=='string' then 
	    if left==right then
		    return true
		else
		    return false
		end
	end
    return isZero(left - right)
end

function isgreater(left, right)
    return isGreaterThanZero(left - right)
end

function isgreaterequal(left, right)
    return not(isLessThanZero(left - right))
end

function isless(left, right)
    return isLessThanZero(left - right)
end

function islessequal(left, right)
    return not(isGreaterThanZero(left - right))
end

function isTrue(v)
    local s = type(v)
    if s == 'boolean' then
        return v
    else
        return v ~= 0
    end
end

function sortedpairs(t,comparator)
    local sortedKeys = {};
    table.foreach(t, function(k,v) table.insert(sortedKeys,k) end);
    table.sort(sortedKeys,comparator);
    local i = 0;
    local function _f(_s,_v)
        i = i + 1;
        local k = sortedKeys[i];
        if (k) then
            return k,t[k];
        end
    end
    return _f,nil,nil;
end

function getweight(stock)
    local v = __stock2Weight[stock]
    if v == nil then
        v = 0
    end
    return v
end

function setweight(weight)
    __stock2Weight = {}
    for k, v in pairs(weight) do
        __stock2Weight[k] = v
    end
end

function gettotalweight()
    local total = 0
    for k, v in pairs(__stock2Weight) do
        if k ~= 0 then
            total = total + v;
        end
    end
    return total
end

function exist1(t, key)
    return t[key] ~= nil
end

function existrange(t, N, key)
    local size = #t
    return size - N < key
end

function removekey(t, key)
    t[key] = nil
end

function toabandon(t, key)
    t[key] = nil
end

function tohold(t, key)
    t[key] = 1
end

function holdingornot(t, val)
    for _, v in pairs(t) do
        if v == val then
            return true
        end
    end
    return false
end

function sortedByKey(test_table)
    local key_table = {}
    local tt={}

    for key,_ in pairs(test_table) do
        table.insert(key_table,key)
    end

    table.sort(key_table)

    return key_table
end



function multi(tbl, keytbl, num, total)
    
    local localTbl=tbl[num]
    local t={}   
    local tt={}
    local ttt={}
    
    for _,v in pairs(keytbl) do
        if t[localTbl[v]] == nil then
            t[localTbl[v]]={}
        end
        table.insert(t[localTbl[v]],v)
    end
    
    for i,v in pairs(t) do
        if #(v) > 1 and num+1 <= total then
            m=multi(tbl,v,num+1,total)
            t[i]=m
        end
    end    
    
    tt=sortedByKey(t)
    
    for _,v in pairs(tt) do 
        n=t[v]
        for ii,vv in pairs(n) do
            table.insert(ttt,vv)
        end
    end
    return ttt
end

function oneTable(tab)
    local tbl={}
    function printTable(tab)
        for i,v in pairs(tab) do
            if type(v) == "table" then
                printTable(v)
            else
                table.insert(tbl,v)
            end
        end 
    end
    printTable(tab)
    return tbl
end

function getKeys(tbl)
    k={}
    for i,v in pairs(tbl) do
        for ii,vv in pairs(v) do
            k[ii]=0
        end
    end
    key={}
    for i,v in pairs(k) do
        table.insert(key,i)
    end
    return key
end

function multisort(...)
    local tttt={}
    local numArgs=select("#", ...)
    local tbl={}
    for i=1 ,numArgs do 
        local arg=select(i, ...)
        table.insert(tbl,arg)
    end
    key = getKeys(tbl)

    final =  multi(tbl,key,1,#tbl)
    return final
end