------------------------------------------------------------
-- 指标函数--与通达信相关
-- 由脚本引擎预先定义，如果有性能问题，可用C++改写
-- @author 
-- @since 2017-3-1
-----------------------------------------------------------
function c_const() 
    local ret
    function const(X)
        if not(X) then
            ret = 0
        else
            ret = X
        end	
        return ret;
    end
    return const
end

function c_inblock()
    local container = FormulaCacheContainer()
    function inblock(sector, timetag, __formula)
        return inblock_c(sector, container, timetag, __formula)
    end
    return inblock
end

function c_inblock2()
    local container = FormulaCacheContainer()
    function inblock(sector, stockcode, timetag, __formula)
        return inblock2_c(sector, stockcode, container, timetag, __formula)
    end
    return inblock
end

function c_sellvol()
    function sellvol(timetag, __formula)
        return buysellvol_c(2, timetag, __formula)
    end
    return sellvol
end

function c_buyvol()
    function buyvol(timetag, __formula)
        return buysellvol_c(1, timetag, __formula)
    end
    return buyvol
end

function c_upnday()
    local history = FormulaDataCacheDouble(0,0)
    local his=FormulaDataCacheDouble(0,0)
    local turn=1
    function upnday(X, M, timetag, style)
        turn =1
        for i=0,M-1,1 do
			if back(X,i, timetag,history,style) < back(X,i+1, timetag,his,style) then
			    turn = 0
				break
		    end
		end
        return turn
    end
    return upnday
end

function c_downnday()
    local history = FormulaDataCacheDouble(0, 0)
    local his=FormulaDataCacheDouble(0, 0)
    function downnday(X, M, timetag, style)
        turn =1
        for i=0,M-1,1 do
			if back(X,i, timetag,history,style) > back(X,i+1, timetag,his,style) then
			    turn = 0
				break
		    end
		end
        return turn
    end
    return downnday
end

function c_nday()
    local history = FormulaDataCacheDouble(0,0)
    local turn=1
    function nday(X, M, timetag, style)
        turn =1
        for i=0,M-1,1 do
			if back(X,i, timetag,history,style)==0 then
			    turn = 0
				break
		    end
		end
        return turn
    end
    return nday
end

function c_turn()
    local cache = TurnDataCache();
    function turn(value, N, timetag, formula)
        return turn_c(value,N,timetag, formula, cache)
    end
    return turn
end

function c_transcationstatic()
    local cache = TransactionCache();
    function transcationstatic(stockcode, type, timetag, formula)
        return transcationstatic_c(stockcode, type, timetag, formula, cache)
    end
    return transcationstatic
end

function c_transcationstaticl1()
    local cache = TransactionCache();
    function transcationstaticl1(stockcode, type, timetag, formula)
        return transcationstaticl1_c(stockcode, type, timetag, formula, cache)
    end
    return transcationstaticl1
end

function c_get_cb_convert_price()
    local cache = CBConvertPriceCache();
    function getCbConvertPrice(bondCode,timetag, formula)
        return get_cb_convert_price_c(bondCode,timetag, formula, cache)
    end
    return getCbConvertPrice
end


function c_external_data()
    local cache = ExternalCache();
    function external_data(data_name,field,period,N,timetag, formula)
        return external_data_c(data_name,field, period,N,timetag, formula, cache)
    end
    return external_data
end

local __pydatacache = {}
function c_callpython()
    local first = true
    function callpython(script, period, stockcode, timetag, formula)
        local _dckey = string.sub(script, 1, string.find(script, '%.'))..period..'.'..stockcode
        local cache = __pydatacache[_dckey]
        if not cache then
            __pydatacache[_dckey] = DataCache()
            cache = __pydatacache[_dckey]
        else
            first = false
        end
        local ret = callpython_c(script, period , stockcode, timetag, formula, first, cache)
        return ret
    end
    return callpython
end

function c_getfindata()
    local cache = FinDataCache()
    function getfindata(value1, value2, session,timetag, formula)
        return getfindata_c(value1,value2,session,timetag, formula, cache)
    end
    return getfindata
end

function c_getfindatabyperiod()
    local container = FormulaCacheContainer()
    function getfindatabyperiod(tablename, colname, year, period, announce, timetag, formula)
        return getfindatabyperiod_c(container, tablename, colname, year, period, announce, timetag, formula)
    end
    return getfindatabyperiod
end

function c_getfindatayear()
    local cache = FormulaDataCacheDouble(0,0)
    function getfindatayear(value1, value2, timetag, formula)
        return getfindatayear_c(value1,value2,timetag, formula, cache)
    end
    return getfindatayear
end

function c_get_longhubang()
    local cache = LonghubangDataCache()
    --value1:filed  value2:direction  value3: rank
    function getlonghubang(value1, value2, value3, timetag,formula) 
        return get_longhubang_c(value1, value2, value3, timetag, formula, cache)
    end
    return getlonghubang
end

function c_get_holderNumber()
    local cache = HolderNumberCache()
    --value:filed 
    function getholdernum(value,timetag,formula) 
        return get_holder_num_c(value,timetag, formula, cache)
    end
    return getholdernum
end


function c_get_top10shareholder()
    local cache = Top10shareholderCache()
    --value1: type ,value2 : filed  value3 rank
    function gettop10shareholder(value1, value2, value3, timetag,formula)
        return get_top10shareholder_c(value1, value2, value3, timetag, formula, cache)
    end
    return gettop10shareholder
end

function c_get_top10shareholderbyperiod()
    local container = FormulaCacheContainer()
    function gettop10shareholderbyperiod(tablename, colname, rank, year, period, announce, type, timetag,formula)
        return get_top10shareholderbyperiod_c(container, tablename, colname, rank, year, period, announce, type, timetag, formula)
    end
    return gettop10shareholderbyperiod
end

function c_gethismaincontract()
    local cache = GetHisMainContractCache()
    function gethismaincontract(value1, timetag, formula)
        return get_his_main_contract_c(value1, timetag, formula, cache)
    end
    return gethismaincontract
end


function c_getrealcontract()
    local cache = GetHisMainContractCache()
    function getrealcontract(value1, timetag, formula)
        return get_real_contract_c(value1, timetag, formula, cache)
    end
    return getrealcontract
end

function c_maincontractchange()
    local cache = GetHisMainContractCache()
    function maincontractchange(value1,timetag,formula)
        return main_contract_change_c(value1,timetag,formula,cache)
    end
    return maincontractchange
end

function c_tickvoldistribution()
    local volnum = 0
	local N = 0
	local cache = TickVolDataCache()
	local ret = 0
	local stayTimetag = 0
	local ratio = 0
	local isFirst = true
    function tickvoldistribution(seconds, ratio, direction, timetag, __formula, style)
		if timetag==0 then
			midret = findoptimumvol(0, ratio, seconds, stayTimetag,__formula, cache, isFirst) 
			isFirst = false
		end
			
		if ratio<=0 or ratio >1 then
			return -1
		end
        if (direction==2 and close(timetag, __formula)==bidprice(timetag, __formula)) then
		    volnum = vol(timetag,__formula)
	    else 
			if(direction==1 and close(timetag, __formula)==askprice(timetag, __formula)) then
				 volnum = vol(timetag,__formula)
			 else 
				if(direction == 0) then
					 volnum = vol(timetag,__formula)
				else 
					 volnum = 0
				end
			end
		end
		
		--用C++找最优解，提高速度
		if volnum > 0 then
			midret = findoptimumvol(volnum, ratio, seconds, stayTimetag,__formula, cache, isFirst) 
		end
		
		
		if midret ~= -1 then 
			ret = midret
		end
			
		if volnum > 0 then
			stayTimetag = stayTimetag + 1
		end
		return ret
    end
    return tickvoldistribution
end

function c_finance()
    local cache = FinDataCache()
    function finance(value,timetag, formula)
        return finance_c(value,timetag,formula,cache)
    end
    return finance
end

function c_buysellvols()
    local cache = QuoterDataCache()
    function buysellvols(value,timetag,formula) 
        return buysellvols_c(value, timetag, formula, cache)
    end
    return buysellvols
end

function c_iopv()
    local cache = QuoterDataCache()
    function iopv(timetag,formula) 
        return iopv_c(cache, timetag, formula)
    end
    return iopv
end

function c_getopenamount()
    local cache = QuoterDataCache()
    function getopenamount(formula) 
        return getopenamount_c(cache,formula)
    end
    return  getopenamount
end

function c_getopenvol()
    local cache = QuoterDataCache()
    function getopenvol(formula) 
        return getopenvol_c(cache,formula)
    end
    return  getopenvol
end

function c_blkname()
    local container = FormulaCacheContainer()
    function blkname(formula)
        return blkname_c(container, "申万一级行业板块", formula)
    end
    return blkname
end

function c_findblock()
    local container = FormulaCacheContainer()
    function findblock(folder, formula)
        return blkname_c(container, folder, formula)
    end
    return findblock
end

function c_findindex()
    local indexTable = {}
    function findindex(sector, stockcode, timetag, formula)
        key = sector..stockcode
        for k, v in pairs(indexTable) do
            if k == key then
                return v
            end
        end
        
        index = findindex_c(sector, stockcode, timetag, formula)
        indexTable[key] = index
        return index
    end
    return findindex
end

function c_switchindex()
    local container = FormulaCacheContainer()
    function funcimpl(stockcode, suffix, timetag, formula)
        return switchindex_c(stockcode, suffix, container, timetag, formula)
    end
    return funcimpl
end

function c_extdatablockrank()
    local cache = ExtFormulaCache()
    function extdatablockrank(name, stockcode, sector, timetag, formula)
        return extdatablockrank_c(name, stockcode, sector, cache, timetag, formula)
    end
    return extdatablockrank
end

function c_extdatablocksum()
    local cache = ExtFormulaCache()
    function extdatablocksum(name, sector, timetag, formula)
        return extdatablocksum_c(name, sector, cache, timetag, formula)
    end
    return extdatablocksum
end

function c_extdatablocksumrange()
    local cache = ExtFormulaCache()
    function funcimpl(name, sector, range, timetag, formula)
        return extdatablocksumrange_c(name, sector, range, cache, timetag, formula)
    end
    return funcimpl
end

function c_extblockranktocode()
    local cache = ExtFormulaCache()
    function funcimpl(name, sector, rate, timetag, formula)
        return extblockranktocode_c(name, sector, rate, cache, timetag, formula)
    end
    return funcimpl
end

function c_blocksize()
    local container = FormulaCacheContainer()
    function blocksize(sector, ...)
        return blocksize_c(container, sector, ...)
    end
    return blocksize
end

function c_stockbyblockrank()
    local container = FormulaCacheContainer()
    function stockbyblockrank(sector, fieldID, rate, timetag, formula)
        return stockbyblockrank_c(sector, fieldID, rate, container, timetag, formula)
    end
    return stockbyblockrank
end

function c_blocksum()
    local container = FormulaCacheContainer()
    function blocksum(sector, fieldID, timetag, formula)
        return blocksum_c(sector, fieldID, container, timetag, formula)
    end
    return blocksum
end

function c_blockrank()
    local container = FormulaCacheContainer()
    function wrapper(sector, stockcode, fieldID, timetag, formula)
        return blockrank_c(sector, stockcode, fieldID, container, timetag, formula)
    end
    return wrapper
end

function c_paramcombcalc()
    local container = FormulaCacheContainer()
    function paramcombcalc(...)
        local a = -1
        local b = -1
        local c = bvector()
        for k,v in ipairs({...}) do
            if k == 1 then
                a = v
            elseif k == 2 then
                b = v
            else
                c:push_back(v)
            end
        end
        return paramcombcalc_c(a, b, c, container)
    end
    return paramcombcalc
end

function c_getoptinfo()
    local container = FormulaCacheContainer()
    function wrapper(optcode, timetag, formula)
        return getoptinfo_c(container, optcode, timetag, formula)
    end
    return wrapper
end

function c_getoptcodebyundl()
    local container = FormulaCacheContainer()
    function wrapper(undlCode, index, timetag, formula)
        return getoptcodebyundl_c(container, undlCode, index, timetag, formula)
    end
    return wrapper
end

function c_getoptcode()
    local container = FormulaCacheContainer()
    function wrapper(optcode, side, price, contractType, timetag, formula)
        return getoptcode_c(container, optcode, side, price, contractType, timetag, formula)
    end
    return wrapper
end

function c_getoptundlcode()
    local container = FormulaCacheContainer()
    function wrapper(optcode, timetag, formula)
        return getoptundlcode_c(container, optcode, timetag, formula)
    end
    return wrapper
end

function c_getoptcodebyno()
    local container = FormulaCacheContainer()
    function wrapper(undlCode, side, contractType, no, day, contractType1, mode, period, timetag, formula)
        local param = ivector()
        param:push_back(contractType)
        param:push_back(no)
        param:push_back(day)
        param:push_back(contractType1)
        param:push_back(mode)
        param:push_back(period)
        param:push_back(1)
        return getoptcodebyno_c(container, undlCode, side, param, timetag, formula)
    end
    return wrapper
end

function c_getoptcodebyno2()
    local container = FormulaCacheContainer()
    function wrapper(undlCode, side, contractType, no, day, contractType1, mode, period, timetag, formula)
        local param = ivector()
        param:push_back(contractType)
        param:push_back(no)
        param:push_back(day)
        param:push_back(contractType1)
        param:push_back(mode)
        param:push_back(period)
        param:push_back(0)
        return getoptcodebyno_c(container, undlCode, side, param, timetag, formula)
    end
    return wrapper
end

function c_getexerciseinterval()
    local container = FormulaCacheContainer()
    function wrapper(undlCode, contractType, timetag, formula)
        return getexerciseinterval_c(container, undlCode, contractType, timetag, formula)
    end
    return wrapper
end

function c_tdate()
    local container = FormulaCacheContainer()
    function wrapper(timetag, formula)
        return tdate_c(container, timetag, formula)
    end
    return wrapper
end

function c_tweekday()
    local container = FormulaCacheContainer()
    function wrapper(timetag, formula)
        return tweekday_c(container, timetag, formula)
    end
    return wrapper
end

function c_timerat()
    local container = FormulaCacheContainer()
    function wrapper(dateNum, timeNum, timetag, formula)
        return timerat_c(container, dateNum, timeNum, timetag, formula)
    end
    return wrapper
end

function c_timerafter()
    local container = FormulaCacheContainer()
    function wrapper(hh, mm, ss, timetag, formula)
        return timerafter_c(container, hh, mm, ss, timetag, formula)
    end
    return wrapper
end

function c_deliveryinterval()
    local container = FormulaCacheContainer()
    function wrapper(timetag, formula)
        return deliveryinterval_c(container, timetag, formula)
    end
    return wrapper
end

function c_deliveryinterval2()
    local container = FormulaCacheContainer()
    function wrapper(stock, timetag, formula)
        return deliveryinterval2_c(container, stock, timetag, formula)
    end
    return wrapper
end

function c_deliveryinterval3()
    local container = FormulaCacheContainer()
    function wrapper(timetag, formula)
        return deliveryinterval3_c(container, timetag, formula)
    end
    return wrapper
end

function c_getcbconversionvalue()
    local container = FormulaCacheContainer()
    function wrapper(code, timetag, formula)
        return getcbconversionvalue_c(container, code, timetag, formula)
    end
    return wrapper
end

function c_getcbconversionpremium()
    local container = FormulaCacheContainer()
    function wrapper(code, timetag, formula)
        return getcbconversionpremium_c(container, code, timetag, formula)
    end
    return wrapper
end

function c_getorderflowdetail()
    local container = FormulaCacheContainer()
    function wrapper(price, index, timetag, formula)
        return getorderflowdetail_c(container, price, index, timetag, formula)
    end
    return wrapper
end

function c_getorderflow()
    local container = FormulaCacheContainer()
    function wrapper(index, timetag, formula)
        return getorderflow_c(container, index, timetag, formula)
    end
    return wrapper
end

function c_getorderflowunbalance()
    local container = FormulaCacheContainer()
    function wrapper(threshold, thresholdTimes, barcount, timetag, formula)
        return getorderflowunbalance_c(container, threshold, thresholdTimes, barcount, timetag, formula)
    end
    return wrapper
end

function c_getorderflowunbalancepricein()
    local container = FormulaCacheContainer()
    function wrapper(threshold, thresholdTimes, barcount, price1, price2, timetag, formula)
        return getorderflowunbalancepricein_c(container, threshold, thresholdTimes, barcount, price1, price2, timetag, formula)
    end
    return wrapper
end

function c_getorderflowpoc()
    local container = FormulaCacheContainer()
    function wrapper(timetag, formula)
        return getorderflowpoc_c(container, timetag, formula)
    end
    return wrapper
end

function c_getorderflowdelta()
    local container = FormulaCacheContainer()
    function wrapper(timetag, formula)
        return getorderflowdelta_c(container, timetag, formula)
    end
    return wrapper
end

function c_getlastfuturemonth()
    local container = FormulaCacheContainer()
    function wrapper(code, index, timetag, formula)
        return getlastfuturemonth_c(container, code, index, timetag, formula)
    end
    return wrapper
end

function c_getlastfuturecode()
    local container = FormulaCacheContainer()
    function wrapper(code, index, timetag, formula)
        return getlastfuturecode_c(container, code, index, timetag, formula)
    end
    return wrapper
end

function c_extdatablocksplitavg()
    local cache = ExtFormulaCache()
    function wrapper(name, sector, total, index, timetag, formula)
        return extdatablocksplitavg_c(name, sector, total, index, cache, timetag, formula)
    end
    return wrapper
end

function c_getcapitalflow()
    local container = FormulaCacheContainer()
    function wrapper(filed, rank, timetag, formula)
        return getcapitalflow_c(filed, rank, timetag, formula, container)
    end
    return wrapper
end

function c_getcapitalflowbyholder()
    local container = FormulaCacheContainer()
    function wrapper(sharedholder, filed, timetag, formula)
        return getcapitalflowbyholder_c(sharedholder, filed, timetag, formula, container)
    end
    return wrapper
end

function c_getfuturecode()
    local container = FormulaCacheContainer()
    function wrapper(code, timetag, formula)
        return getfuturecode_c(container, code, timetag, formula)
    end
    return wrapper
end

function c_winner()
     local container = FormulaCacheContainer()
     function wrapper(price,timetag,formula)
         return winner_cost_c(price,0,container,timetag,formula)
     end
     return wrapper
end

function c_cost()
     local container = FormulaCacheContainer()
     function wrapper(price,timetag,formula)
         return winner_cost_c(price,1,container,timetag,formula)
     end
     return wrapper
end

function c_findblocklist()
    local container = FormulaCacheContainer()
    function wrapper(folder, formula)
        return findblocklist_c(container, folder, formula)
    end
    return wrapper
end

function c_unitofquantity()
    local container = FormulaCacheContainer()
    function wrapper(code, formula)
        return unitofquantity_c(code, container, formula)
    end
    return wrapper
end

function c_equalweightindex()
    local container = FormulaCacheContainer()
    function wrapper(code, formula)
        return equalweightindex_c(code, container, formula)
    end
    return wrapper
end

function c_isindexorglr()
    local container = FormulaCacheContainer()
    function wrapper(code, formula)
        return isindexorglr_c(code, container, formula)
    end
    return wrapper
end

function c_isetfcode()
    local container = FormulaCacheContainer()
    function wrapper(code, formula)
        return isetfcode_c(container, code, formula)
    end
    return wrapper
end

function c_isindexcode()
    local container = FormulaCacheContainer()
    function wrapper(code, formula)
        return isindexcode_c(container, code, formula)
    end
    return wrapper
end

function c_isfuturecode()
    local container = FormulaCacheContainer()
    function wrapper(code, formula)
        return isfuturecode_c(container, code, formula)
    end
    return wrapper
end

function c_upstopprice()
    local container = FormulaCacheContainer()
    function upstopprice(stockcode, timetag, formula)
        return stopprice_c(container, stockcode, 1, timetag, formula)
    end
    return upstopprice
end

function c_downstopprice()
    local container = FormulaCacheContainer()
    function downstopprice(stockcode, timetag, formula)
        return stopprice_c(container, stockcode, 2, timetag, formula)
    end
    return downstopprice
end

function c_dividfactor()
    local container = FormulaCacheContainer()
    function func(type, timetag, formula)
        return dividfactor_c(container, type, timetag, formula)
    end
    return func
end

function c_getinstrumentdetail()
    local container = FormulaCacheContainer()
    function wrapper(stockcode, fieldname, timetag, formula)
        return getinstrumentdetail_c(container, stockcode, fieldname, timetag, formula)
    end
    return wrapper
end

function c_limitupperformance()
    local container = FormulaCacheContainer()
    function wrapper(stockcode, type, timetag, formula)
        return limitupperformance_c(container, stockcode, type, timetag, formula)
    end
    return wrapper
end

function c_fundnetvalue()
    local container = FormulaCacheContainer()
    function wrapper(stockcode, type, timetag, formula)
        return fundnetvalue_c(container, stockcode, type, timetag, formula)
    end
    return wrapper
end

function c_get_etf_statistics()
    local container = FormulaCacheContainer()
    function fun(stockcode, field, timetag, formula)
        return get_etf_statistics_c(container, 1, stockcode, field, timetag, formula)
    end
    return fun
end

function c_get_etf_statisticsl2()
    local container = FormulaCacheContainer()
    function fun(stockcode, field, timetag, formula)
        return get_etf_statistics_c(container, 2, stockcode, field, timetag, formula)
    end
    return fun
end