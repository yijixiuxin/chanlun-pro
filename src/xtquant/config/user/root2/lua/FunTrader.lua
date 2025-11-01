------------------------------------------------------------
-- 程序化交易函数
-- 由脚本引擎预先定义，如果有性能问题，可用C++改写
-- @author zhangjin
-- @since 2012-10-18
-----------------------------------------------------------
function c_order()
    local history = FormulaDataCacheDouble(0, 0);
    local lastBasket = ''
    function order(opType, chanel, addr, basket, timetag, formula)
        if basket == nil then
            basket = lastBasket
        end
        local ret = placeorder_c(opType, chanel, addr, basket, timetag, formula, history)
        return ret
    end
    return order
end

function c_passorder()
    local history = FormulaDataCacheDouble(0, 0);
    --orderCode:basketName or stockID
    function passorder(opType, orderType, accountid, orderCode, prType, price, volume, quickTrade, strategyName, userOrderId, timetag, formula)
        --由于luabind的限制不能处理超过10个参数，这里把accountid和strategyName合并在一起，到了C++里再拆分 
        accidAndstrName = accountid.."#"..strategyName.."#"..tostring(quickTrade).."#"..userOrderId
        local ret = passorder_c(opType, orderType, accidAndstrName, orderCode, prType, price, volume, timetag, formula, history)
        return ret
    end
    return passorder
end

function passorder(
    optype, ordertype, accountid, accounttype
    , marketstock, pricetype, price, volume
    , strategyname, quicktrade, remark
    , timetag, formula
)
    ptable = {
        quicktrade = quicktrade
        , strategyname = strategyname
        , remark = remark
        , barpos = timetag
    }

    return passorder2_c(
        optype, ordertype, accountid, accounttype
        , marketstock, pricetype, price, volume
        , ptable, formula
    )
end

function c_trade()
    local history = FormulaDataCacheDouble(0, 0);
    local tp = tradeparam()
    function trade_c(param, address, timetag, formula)
        if param then
            tp = copyParam(param)
            trade(param, 0, timetag, formula, history, address)
        else
            trade(tp, 1, timetag, formula, history, address)
        end
    end
    return trade_c
end

function c_hedgestocktrade()
    local history = FormulaDataCacheDouble(0, 0);
    local tp = tradeparam()
    function hedgestocktrade_c(param, address, timetag, formula)
        if param then
            tp = copyParam(param)
            hedgestocktrade(param, 0, timetag, formula, history, address)
        else
            hedgestocktrade(tp, 1, timetag, formula, history, address)
        end
    end
    return hedgestocktrade_c
end

function c_cancel()
    local history = FormulaDataCacheDouble(0, 0)
    function cancel_c(codeNumber, timetag)
        cancel(codeNumber, history)
    end
    return cancel_c
end

function c_writeorder()
    local history = FormulaDataCacheDouble(0, 0)
    function writeorder_c(filepath, content, timetag, formula)
        return writeorder(filepath, content, timetag, formula, history)
    end
    return writeorder_c
end

function positionadjust(positions, weight, channel)
    local s = '35,'..channel..'\n'
    for i = 0, positions:size() - 1, 1 do
        local detail = positions:at(i)
        local adjustedVol = detail.m_nVolume * weight
        s = s..detail.m_strInstrumentID..'\t'..adjustedVol..'\n'
    end
    return s
end

function c_portfoliosell(type)
    local history = FormulaDataCacheDouble(0, 0)
    function portfoliosell_c(type, timetag, formula)
        return portfoliosell(type, timetag, formula, history)
    end
    return portfoliosell_c
end

function c_portfoliobuy()
    local history = FormulaDataCacheDouble(0, 0);
    local lastBasket = ''
    function portfoliobuy(opType, chanel, addr, basket, timetag, formula)
        if basket == nil then
            basket = lastBasket
        end
        return portfoliobuy_c(opType, chanel, addr, basket, timetag, formula, history)
    end
    return portfoliobuy
end

function algo_passorder(
    optype, ordertype, accountid, accounttype
    , marketstock, pricetype, price, volume
    , strategyname, quicktrade, remark
    , algoname
    , timetag, formula
)
    ptable = {
        quicktrade = quicktrade
        , strategyname = strategyname
        , remark = remark
        , barpos = timetag
        , algoname = algoname
    }

    return algo_passorder_c(
        optype, ordertype, accountid, accounttype
        , marketstock, pricetype, price, volume
        , ptable, formula
    )
end

function cancel_task(taskID, accountID, accountType)
    return cancel_task_c(taskID, accountID, accountType, timetag, formula)
end

function c_readsignal()
    local container = FormulaCacheContainer()
    function wrapper(filePath, stockCode, timetag, formula)
       return readsignal_c(filePath, stockCode, container, timetag, formula)
    end
    return wrapper
end

function c_drawsignal()
    local container = FormulaCacheContainer()
    function wrapper(cond, signalType, drawPrice, timetag, formula)
       return drawsignal_c(container, cond, signalType, drawPrice, timetag, formula)
    end
    return wrapper
end

function c_cmdprogress()
    local container = FormulaCacheContainer()
    function wrapper(cmdID, timetag, formula)
       return cmdprogress_c(container, cmdID, timetag, formula)
    end
    return wrapper
end

function c_cmdstatus()
    local container = FormulaCacheContainer()
    function wrapper(cmdID, timetag, formula)
       return cmdstatus_c(container, cmdID, timetag, formula)
    end
    return wrapper
end
