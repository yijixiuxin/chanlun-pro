------------------------------------------------------------
-- 逻辑函数
-- 由脚本引擎预先定义，如果有性能问题，可用C++改写
-- @author zhangjin
-- @since 2012-9-18
-----------------------------------------------------------
function c_any()
    local count = 0
    local history = FormulaDataCacheBool(1)
    local ret
    function any(condition, N, timetag, type)
        count, ret = all_c(not(condition), N, count, timetag, type, history)
        return not(ret)
    end
    return any
end

function c_exist()
    return c_any()
end

function c_all()
    local count = 0
    local history = FormulaDataCacheBool(1)
    local ret
    function all(condition, N, timetag, type)
        count, ret = all_c(condition, N, count, timetag, type, history)
        return ret
    end
    return all
end

--条件跟随函数
function c_valuewhen()
    local lastValue = 0 / 0
    local ret
    local first = true
    local lastTimetag = 0
    function valuewhen(condition, value, timetag)
        if condition then
            ret = value
        else
            ret = lastValue
        end
        if (lastTimetag ~= timetag) then
            lastValue = ret
            lastTimetag = timetag
        end
        return ret
    end
    return valuewhen
end

function c_cross()
    local lastV1 = 0
    local lastV2 = -1
    local lastTime = -1
    local t1 = 0
    local t2 = -1
    local count = 0
    function cross(v1, v2, timetag)
        if timetag ~= lastTime then
            lastTime = timetag
            count = 0
            t1 = lastV1
            t2 = lastV2
        end
        count = count + 1
        if count > 1 then
            lastV1 = t1
            lastV2 = t2
        end
        local ret = cross_c(v1, v2, lastV1, lastV2)
        lastV1 = v1
        lastV2 = v2
        return ret
    end
    return cross
end

function iff(condition, v1, v2)
    --print(type(v1),type(v2))
    if condition then
        return v1;
    else
        return v2;
    end
end

function ifelse(condition, v1, v2)
    if condition then
        return v1;
    else
        return v2;
    end
end

function ifn(X, A, B)
    if X then
        return B
    else
        return A
    end
end

function valid(value)
    return isValid(value)
end

--todo: 当A, B, C中出现无效值时，金字塔返回无效值，该函数返回false
--function between(A, B, C)
    --if (A - B >= 1e-6 and A - C <= 1e-6) or (A - B <= 1e-6 and A - C >= 1e-6) then
        --return true
    --else
        --return false
    --end
--end

--todo 这三个函数是隐藏的行情函数
--function isdown(timetag, __formula)
    --if close(timetag, __formula) - open(timetag, __formula) < 1e-6 then
        --return true
    --else
        --return false
    --end
--end
--
--function isequal(timetag, __formula)
    ----if close(timetag, __formula) == open(timetag, __formula) then
    --if math.fabs(close(timetag, __formula) - open(timetag, __formula)) < 1e-6 then
        --return true
    --else
        --return false
    --end
--end
--
--function isup(timetag, __formula)
    --if close(timetag, __formula) - open(timetag, __formula) > 1e-6 then
        --return true
    --else
        --return false
    --end
--end

function islastbar(timetag, __formula)
    return timetag == __formula:getLastBar()
end

function c_last()
    local history = FormulaDataCacheBool(1)
    local count = 0
    local ret
    function last(X, A, B, timetag)
        count, ret = last_c(X, A, B, timetag, count, history)
        return ret
    end
    return last
end
--[[
function c_longcross(A, B, N)
    local historyA = FormulaDataCacheDouble(0)
    local historyB = FormulaDataCacheDouble(0)
    local lessCount = 0
    function longcross(A, B, N, type)
        lessCount, ret = longcross_c(A, B, N, type, historyA, historyB, lessCount)
        return ret
    end
    return longcross
end
]]--

function c_longcross(A, B, N) --jch
    local lessCount = 0
    local tmplessCount = 0
    local lastTimetag = -1
    function longcross(A, B, N, timetag)
        local ret = false
        local condition = A < B
        if lastTimetag ~= timetag then
            tmplessCount = lessCount
            lastTimetag = timetag
        end
        if condition then
            lessCount = tmplessCount + 1
        else
            if lessCount >= N then
                ret = true
            end
            lessCount = 0        
        end
        return ret
    end
    return longcross
end

function range(A, B, C)
    if A - B > 1e-6 and A - C< 1e-6 then
        return true
    else
        return false
    end
end

function c_orderdirection()
    local cache = LastValueCache()
    function orderdirection(timetag,formula)
        return orderdirection_c(timetag, formula, cache)
    end
    return orderdirection
end

function c_isbuyorder()
    local cache = LastValueCache()
    function orderdirection(timetag,formula)
        local val = orderdirection_c(timetag, formula, cache)
        if val == 1 then
            return 1
        else
            return 0
        end
        return 0
    end
    return orderdirection
end

function c_issellorder()
    local cache = LastValueCache()
    function issellorder(timetag,formula)
        local val = orderdirection_c(timetag, formula, cache)
        if val == -1 then
            return 1
        else
            return 0
        end
        return 0
    end
    return issellorder
end