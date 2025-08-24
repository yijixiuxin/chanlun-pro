------------------------------------------------------------
-- 引用函数
-- 由脚本引擎预先定义，如果有性能问题，可用C++改写
-- @author zhangjin
-- @since 2012-9-18
-----------------------------------------------------------

function c_ref()
    local history = FormulaDataCacheDouble(0, 0)
    function ref(X, distance, timetag, style)
        if not(X) then
            X = 0
        end
        return ref_c(X, distance, timetag, history, style)
    end
    return ref
end

function c_barslast()
    local lastTrue = -1
    local lastTrue_1 = -1
    local curTimetag = -1
    function barslast(condition, timetag)
        if curTimetag ~= timetag then
            curTimetag = timetag
            lastTrue_1 = lastTrue
        else
            lastTrue = lastTrue_1
        end
        
        if not(condition) then
            condition = false
        end
        
        if condition then
            lastTrue = curTimetag
        end
        
        if lastTrue == -1 then
            return 0
        else
            return curTimetag - lastTrue
        end
    end
    return barslast
end

function c_barslasts()
    local container = FormulaCacheContainer()
    function wrapper(condition, N, timetag)
        return barslasts_c(condition, N, container, timetag)
    end
    return wrapper
end

function c_count()
    local history = FormulaDataCacheBool(0, 0)
    function count(condition, N, timetag, type)
        if not(condition) then
            condition = false
        end
        return count_c(condition, N, timetag, history, type)
    end
    return count
end

function c_ma()
    local history = FormulaDataCacheDouble(0, 0)
    function ma(X, N, timetag, type)
        if not(X) then
            X = 0
        end
        local avg = ma_c(X, N, timetag, history, type)
        return avg;
    end
    return ma
end

function c_xma()
    local history = FormulaDataCacheDouble(0, 0)
    function xma(X, N, timetag, __formula, type)
        if not(X) then
            X = 0
        end
        local avg = xma_c(X, N, timetag, history, __formula, type)
        return avg;
    end
    return xma
end

function c_ima()
    local history = FormulaDataCacheDouble(0, 0)
    function ima(X, N, S, timetag, type)
        if not(X) then
            X = 0
        end
        return ima_c(X, N, S, timetag, history, type)
    end
    return ima
end


--求动态移动平均 jch
function c_dma()
    local last
    local lasttimetag = -1
    local ret = math.huge
    function dma(X, A, timetag)
        if not(X) then
            X = 0
        end
        if lasttimetag ~= timetag then --主推数据
            last = ret
            lasttimetag = timetag
        end
        if not(isValid(last)) then
            last = X
        end
        if (A > 1 or A <= 0) then
            A = 1
        end
        ret = A * X + (1 - A) * last
        return ret;
    end
    return dma
end

--[[求动态移动平均
function c_dma()
    local last = 0
    local ret
    function dma(X, A, timetag)
        last, ret = dma_c(X, last, A, timetag);
        return ret;
    end
    return dma
end]]--

--求指数平滑移动平均 jch
function c_ema()
    local last
    local lasttimetag = -1
    local ret = math.huge
    function ema(X, N, timetag)
        if not(X) then
            X = 0
        end
        if lasttimetag ~= timetag then --主推数据
            last = ret
            lasttimetag = timetag
        end
        if not(isValid(last)) then
            last = X
        end
        ret = (2 * X + (N - 1) * last) / (N + 1)
        return ret;
    end
    return ema
end

--[[求指数平滑移动平均
function c_ema()
    local history = FormulaDataCacheDouble(0, 0)
    function ema(X, N, timetag)
        ret = ema_c(X, last, N, timetag);
        return ret;
    end
    return ema
end
function c_sma() 
    local last = math.huge
    local lasttimetag = -1
    local ret = 0
    function sma(X, N, M, timetag)
        if timetag == 0 and isValid(X) then
            last = X
        end
        if not(isValid(X)) then
            X = math.huge
        end
        local ret = (X * M + (N - M) * last) / N;
        if isValid(X) and not(isValid(ret)) then
            last = X
        else
            last = ret
        end
        return ret
    end
    return sma
end
]]--
--移动平均 jch
function c_sma()
    local last = math.huge
    local lasttimetag = -1
    local ret = math.huge
    function sma(X, N, M, timetag)
        if not(X) then
            X = 0
        end
        if lasttimetag ~= timetag then
            last = ret
            lasttimetag = timetag
        end
        if not(isValid(last)) then
            last = X
        end
        ret = (M * X + (N - M) * last) / N
        return ret
    end
    return sma
end

--递归移动平均 jch
function c_tma()
    local last
    local lasttimetag = -1
    local ret = math.huge
    function tma(X, N, M, timetag)
        if not(X) then
            X = 0
        end
        if lasttimetag ~= timetag then
            last = ret
            lasttimetag = timetag
        end
        if not(isValid(last)) then
            last = X
        end
        ret = N * last + M * X
        return ret
    end
    return tma
end
--[[递归移动平均
function c_tma()
    local last = 0
    function tma(X, N, M, timetag)
        if not(isValid(X)) then
            X = math.huge;
        end
        local ret = M * X + N * last;
        last = ret
        return ret
    end
    return tma
end]]--

function c_sum()
    local history = FormulaDataCacheDouble(1)
    function sum(X, N, timetag, type)
        if not(X) then
            X = 0
        end
        local ret = sum_c(X, N, history, timetag, type)
        return ret
    end
    return sum
end

function c_hhv()
    local history = FormulaDataCacheDouble(-math.huge, -1)
    function hhv(X, N, timetag, style)
        if not(X) then
            X = 0
        end
        local index, max = hhv_c(X, N, timetag, history, style)
        return max
    end
    return hhv
end

function c_hhvbars()
    local history = FormulaDataCacheDouble(-math.huge, -1)
    function hhvbars(X, N, timetag, style)
        if not(X) then
            X = 0
        end
        local index, max = hhv_c(X, N, timetag, history, style)
        return index
    end
    return hhvbars
end

function c_llv()
    local history = FormulaDataCacheDouble(math.huge, -1)
    function llv(X, N, timetag, type)
        if not(X) then
            X = 0
        end
        local index, min = llv_c(X, N, timetag, history, type)
        return min
    end
    return llv
end

function c_llvbars()
    local history = FormulaDataCacheDouble(math.huge, -1)
    function llvbars(X, N, timetag, type)
        if not(X) then
            X = 0
        end
        local index, min = llv_c(X, N, timetag, history, type)
        return index
    end
    return llvbars
end

function c_filter()
    local lastTrue = -1
    local lastTimetag = -1
    local realTimeLastTrue = -1
    function filter(val, N, timetag)
        local ret = 0
        if timetag ~= lastTimetag then
            lastTrue = realTimeLastTrue
        end
        if timetag - lastTrue > N then
            ret = val
            if val > 0 then
                realTimeLastTrue = timetag
            else
                realTimeLastTrue = lastTrue
            end
        end
        lastTimetag = timetag
        return ret
    end
    return filter
end

function c_sfilter()
    local lastX = 0
    local lastCond = 0
    function sfilter(X, cond, timetag)
        if cond then
            lastCond = timetag
            if X then
                lastX = timetag
            end
            return X
        else
            if lastX > lastCond then
                return false
            else
                if X then
                    lastX = timetag
                end
                return X
            end
        end
    end
    return sfilter
end

function c_barscount()
    local isValid = false
    local first = 0
    function barscount(X, timetag)
        if isValid then
            return timetag - first + 1
        elseif valid(X) then
            isValid = true
            first = timetag
            return 1
        else
            return 0 / 0
        end
    end
    return barscount
end

function c_barssincen()
    local isFirst = true
    local index = 0
	local ret = 0
	local indexArray = {}--存放满足条件的timetag
	local indFirst = 0 --indexArray存放timetag的开始位 ，indFirst>=indLast时说明没有满足条件的timetag
	local indLast = 0 --indexArray存放timetag的结束位
    function barssincen(condition,n,timetag)
	    if n < 2 then 
		    return 0
		end
	    ret = 0
        if  timetag >= n-1 and (not isFirst) then --timetag 是从0开始的
		    if timetag - index > n-1  then
			    if indFirst < indLast then
					indFirst = indFirst + 1
					index = indexArray[indFirst]
					ret = timetag - index
					indexArray[indFirst] = nil --释放空间			
				end				
            else
                ret = timetag - index			
			end 			
		end
        if condition then
            indLast = indLast + 1		
			indexArray[indLast] = timetag			
			if isFirst then
			    isFirst = false
			    index = timetag
			    if index == 0 then --如果第一个K线就满足条件,在此做数组删除处理（避免下个周期还读到index=0）
                    indexArray[indLast] = nil
                    indLast = indLast - 1
               	end			  
			end			
        end
        
		return ret
    end
    return barssincen
end

function c_barssince()
    local isFirst = false
    local index = 0
    function barssince(condition, timetag)
        if isFirst then
            return timetag - index
        elseif condition then
            isFirst = true
            index = timetag
            return 0
        else
            return 0
        end
    end
    return barssince
end

function c_barssincen()
    local isFirst = true
    local index = 0
	local ret = 0
	local indexArray = {}--存放满足条件的timetag
	local indFirst = 0 --indexArray存放timetag的开始位 ，indFirst>=indLast时说明没有满足条件的timetag
	local indLast = 0 --indexArray存放timetag的结束位
    function barssincen(condition,n,timetag)
	    if n < 2 then 
		    return 0
		end
	    ret = 0
        if  timetag >= n-1 and (not isFirst) then --timetag 是从0开始的
		    if timetag - index > n-1  then
			    if indFirst < indLast then
					indFirst = indFirst + 1
					index = indexArray[indFirst]
					ret = timetag - index
					indexArray[indFirst] = nil --释放空间			
				end				
            else
                ret = timetag - index			
			end 			
		end
        if condition then
            indLast = indLast + 1		
			indexArray[indLast] = timetag			
			if isFirst then
			    isFirst = false
			    index = timetag
			    if index == 0 then --如果第一个K线就满足条件,在此做数组删除处理（避免下个周期还读到index=0）
                    indexArray[indLast] = nil
                    indLast = indLast - 1
               	end			  
			end			
        end
        
		return ret
    end
    return barssincen
end

function currbarscount(timetag, __formula)
    local endPos = __formula:getLastBar()
    return endPos - timetag + 1
end

function drawnull()
    return 0 / 0
end
    
function c_tr()
    local lastClose = 0 / 0
    function tr(timetag, __formula)
        local c = close(timetag, __formula)
        local h = high(timetag, __formula)
        local l = low(timetag, __formula)
        local v1 = h - l
        local v2 = h - lastClose
        local v3 = lastClose - l
        lastClose = c
        if v1 > v2 then
            if v1 > v3 then
                return v1
            else
                return v3
            end
        else
            if v2 > v3 then
                return v2
            else
                return v3
            end
        end
    end
    return tr
end

function c_wma()
    local history = FormulaDataCacheDouble(2)
    --local sum = 0
    --local weightSum = 0
    local avg
    function wma(X, N, timetag, tp)
        if not(X) then
            X = 0
        end
        --sum, weightSum, avg = wma_c(X, N, timetag, sum, weightSum, history)
        avg = wma_c(X, N, timetag, history, tp)
        return avg
    end
    return wma
end

function c_trma()
    local ma1 = c_ma()
    local ma2 = c_ma()
    function trma(X, N, timetag, type)
        if not(X) then
            X = 0
        end
        local N1
        local temp, ret
        if N % 2 == 1 then
            N1 = (N + 1) / 2
            temp = ma1(X, N1, timetag, type)
            ret = ma2(temp, N1, timetag, type)
        else
            N1 = N / 2
            temp = ma1(X, N1, timetag, type)
            ret = ma2(temp, N1 + 1, timetag, type)
        end
        return ret
    end
    return trma
end
    
function c_ret()
    local history = FormulaDataCacheDouble(0)
    local lastValue
    function ret(X, A, timetag, __formula, type)
        if not(X) then
            X = 0
        end
        lastValue = ret_c(X, A, timetag, __formula, history, type)
        return lastValue
    end
    return ret
end

function c_newhbars()
    local history = FormulaDataCacheDouble(0)
    function newhbars(X, N, timetag, type)
        if not(X) then
            X = 0
        end
        return newhbars_c(X, N, timetag, history, type)
    end
    return newhbars
end

function c_newlbars()
    local history = FormulaDataCacheDouble(0)
    function newlbars(X, N, timetag, type)
        if not(X) then
            X = 0
        end
        --return newlbars_c(X, N, history)
        return newhbars_c( -1 * X, N, timetag, history, type)
    end
    return newlbars
end

function c_hod()
    local history = FormulaDataCacheDouble(0)
    function hod(X, N, timetag)
        if not(X) then
            X = 0
        end
        return hod_c(X, N, timetag, history)
    end
    return hod
end

function c_lod()
    local history = FormulaDataCacheDouble(0)
    function lod(X, N, timetag)
        if not(X) then
            X = 0
        end
        return hod_c( -1 * X, N, timetag, history)
    end
    return lod
end
--[[
function c_sumbars()
    local history = FormulaDataCacheDouble(2)
    local sum = 0.0
    local period = 0
    function sumbars(X, A)
        sum, period = sumbars_c(X, A, sum, period, history)
        return period
    end
    return sumbars
end]]--
function c_sumbars()
    local history = FormulaDataCacheDouble(0,0)
    function sumbars(X, A, timetag)
        if not(X) then
            X = 0
        end
        local ret = sumbars_c(X, A, timetag, history)
        return ret
    end
    return sumbars
end

function c_barslastcount()
    local container = FormulaCacheContainer()
    function wrapper(condition, timetag, __formula)
        return barslastcount_c(container, condition, timetag, __formula)
    end
    return wrapper
end

function c_mema()
    local last
    local lasttimetag = -1
    local ret = math.huge
    function mema(X, N, timetag)
        if not(X) then
            X = 0
        end
        if lasttimetag ~= timetag then --主推数据
            last = ret
            lasttimetag = timetag
        end
        if not(isValid(last)) then
            last = X
        end
        ret = (X + (N - 1) * last) / N
        return ret;
    end
    return mema
end

