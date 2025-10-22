------------------------------------------------------------
-- 指标函数
-- 由脚本引擎预先定义，如果有性能问题，可用C++改写
-- @author zhangjin
-- @since 2012-10-8
-----------------------------------------------------------
function c_sar()
    local cache = FormulaCacheContainer()
    function sar(N, S, M, timetag, __formula)
        return sar_c(cache, N, S, M, timetag, __formula)
    end
    return sar
end

function c_sarturn()
    local cache = FormulaCacheContainer()
    function sarturn(N, S, M, timetag, __formula)
        return sarturn_c(cache, N, S, M, timetag, __formula)
    end
    return sarturn
end

function callstock2()
    local container = FormulaCacheContainer()
    function funcimpl(stockcode, metaID, fieldID, period, offset, timetag, formula)
        return callstock2_c(container, stockcode, metaID, fieldID, period, offset, timetag, formula)
    end
    return funcimpl
end

function getstocklist()
    local container = FormulaCacheContainer()
    function funcimpl(sector, timetag, formula)
        return getstocklist_c(container, sector, timetag, formula)
    end
    return funcimpl
end

function getinitgroup()
    local container = FormulaCacheContainer()
    function funcimpl(timetag, formula)
        return getinitgroup_c(container, timetag, formula)
    end
    return funcimpl
end

function c_getspotprodgroup()
    local container = FormulaCacheContainer()
    function funcimpl(productcode, timetag, formula)
        return getspotprodgroup_c(container, productcode, timetag, formula)
    end
    return funcimpl
end

function c_getspotprodinst()
    local container = FormulaCacheContainer()
    function funcimpl(productcode, stockindex, timetag, formula)
        return getspotprodinst_c(container, productcode, stockindex, timetag, formula)
    end
    return funcimpl
end

function c_getwarehousereceipt()
    local container = FormulaCacheContainer()
    function funcimpl(productcode, warehousecode, timetag, formula)
        return getwarehousereceipt_c(container, productcode, warehousecode, timetag, formula)
    end
    return funcimpl
end

function c_getwarehousename()
    local container = FormulaCacheContainer()
    function funcimpl(productcode, warehouseindex, timetag, formula)
        return getwarehousename_c(container, productcode, warehouseindex, timetag, formula)
    end
    return funcimpl
end

function c_getfutureseats()
    local container = FormulaCacheContainer()
    function funcimpl(stockcode, field, rank, timetag, formula)
        return getfutureseats_c(container, stockcode, field, rank, timetag, formula)
    end
    return funcimpl
end

function c_getfutureseatsname()
    local container = FormulaCacheContainer()
    function funcimpl(stockcode, field, rank, timetag, formula)
        return getfutureseatsname_c(container, stockcode, field, rank, timetag, formula)
    end
    return funcimpl
end

function c_findfutureseats()
    local container = FormulaCacheContainer()
    function funcimpl(stockcode, field, member, timetag, formula)
        return findfutureseats_c(container, stockcode, field, member, timetag, formula)
    end
    return funcimpl
end

function c_stocktype()
    local container = FormulaCacheContainer()
    function funcimpl(stockcode, timetag, formula)
        return stocktype_c(container, stockcode, timetag, formula)
    end
    return funcimpl
end

function c_convindex()
    local container = FormulaCacheContainer()
    function funcimpl(stockcode, type, timetag, formula)
        return convindex_c(container, stockcode, type, timetag, formula)
    end
    return funcimpl
end
