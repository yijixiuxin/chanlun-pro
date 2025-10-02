------------------------------------------------------------
-- 统计函数
-- 由脚本引擎预先定义，如果有性能问题，可用C++改写
-- @author zhangjin
-- @since 2012-9-18
-----------------------------------------------------------

--判别调用
function poisson(v1,v2,v3,timetag,formula)
    if (type(v3) == "number")
    then if (v3>0)
        then return poisson_c(v1,v2,true,timetag,formula)
        else return poisson_c(v1,v2,false,timetag,formula)
        end
    end
    return poisson_c(v1,v2,v3,timetag,formula)
end

function weibull(v1,v2,v3,v4,timetag,formula)
    return weibull_c(v1,v2,v3,v4,timetag,formula)
end

function expondist(v1,v2,v3,timetag,formula)
    return expondist_c(v1,v2,v3,timetag,formula)
end

function binomdist(v1,v2,v3,v4,timetag,formula)
    return binomdist_c(v1,v2,v3,v4,timetag,formula)
end


cacheDoubleNum = 4
--drl2 曲线回归偏离度
function c_drl2()
    local cache1 = FormulaDataCacheDouble(15)
    local cache2 = FormulaDataCacheDouble(15)
    local cache3 = FormulaDataCacheDouble(15)
    function drl2_func(value, N, timetag, __formula)
        local ret = drl2(value, cache1, cache2, cache3, N, timetag, __formula)
        return ret
    end
    return drl2_func
end
--forecast2 二次曲线回归预测值。
function c_forecast2()
    local cache1 = FormulaDataCacheDouble(10)
    local cache2 = FormulaDataCacheDouble(10)
    local cache3 = FormulaDataCacheDouble(10)
    function forecast2_func(value, N, timetag, __formula)
        local ret = forecast2(value, cache1, cache2, cache3, N, timetag, __formula)
        return ret
    end
    return forecast2_func
end
--slope 曲线回归相关系数
function c_slope20()
    local cache1 = FormulaDataCacheDouble(10)
    local cache2 = FormulaDataCacheDouble(10)
    local cache3 = FormulaDataCacheDouble(10)
    function slope20_func(value, N, timetag, __formula)
        local ret = slope20(value, cache1, cache2, cache3,N, timetag, __formula)
        return ret
    end
    return slope20_func
end
function c_slope21()
    local cache1 = FormulaDataCacheDouble(10)
    local cache2 = FormulaDataCacheDouble(10)
    local cache3 = FormulaDataCacheDouble(10)
    function slope21_func(value, N, timetag, __formula)
        local ret = slope21(value, cache1, cache2, cache3, N, timetag, __formula)
        return ret
    end
    return slope21_func
end
function c_slope22()
    local cache1 = FormulaDataCacheDouble(10)
    local cache2 = FormulaDataCacheDouble(10)
    local cache3 = FormulaDataCacheDouble(10)
    function slope22_func(value, N, timetag, __formula)
        local ret = slope22(value, cache1, cache2, cache3, N, timetag, __formula)
        return ret
    end
    return slope22_func
end
--drl 直线回归偏离度
function c_drl()
    local cachey = FormulaDataCacheDouble(cacheDoubleNum)
    local cachex = FormulaDataCacheDouble(cacheDoubleNum)
    function drl_func(value, N, timetag, __formula)
        local ret = drl(value, cachey, cachex, N, timetag, __formula)
        return ret
    end
    return drl_func
end
--forecast 线性回归预测值。
function c_forecast()
    local cachey = FormulaDataCacheDouble(cacheDoubleNum)
    local cachex = FormulaDataCacheDouble(cacheDoubleNum)
    function forecast_func(value, N, timetag, __formula)
        local ret = forecast(value, cachey, cachex, N, timetag, __formula)
        return ret
    end
    return forecast_func
end
--slope 线性回归斜率
function c_slope()
    local cachey = FormulaDataCacheDouble(cacheDoubleNum)
    local cachex = FormulaDataCacheDouble(cacheDoubleNum)
    function slope_func(value, N, timetag, __formula)
        local ret = slope(value, cachey, cachex, N, timetag, __formula)
        return ret
    end
    return slope_func
end
--percentrank 返回特定数值在数据集中的百分比排位
function c_percentrank()
    local cache = FormulaOrderCache()
    function percentrank_func(value, N, x, significance, timetag, __formula)
        local ret = percentrank(value, cache, N, x, significance, timetag, __formula)
        return ret
    end
    return percentrank_func
end
--percentile 返回区域中数值的第 K 个百分点的值
function c_percentile()
    local cache = FormulaOrderCache()
    function percentile_func(value, N, k, timetag, __formula)
        local ret = percentile(value, cache, N, k, timetag, __formula)
        return ret
    end
    return percentile_func
end
--median 返回区域中数值的中位数
function c_median()
    local cache = FormulaOrderCache()
    function median_func(value, N, timetag, __formula)
        local ret = percentile(value, cache, N, 0.5, timetag, __formula)
        return ret
    end
    return median_func
end
--trimmean 返回数据的内部平均值
function c_trimmean()
    local cache = FormulaOrderCache()
    function trimmean_func(value, N, percent, timetag, __formula)
        local ret = trimmean(value, cache, N, percent, timetag, __formula)
        return ret
    end
    return trimmean_func
end
--quartile 返回数据的四分位数
function c_quartile()
    local cache = FormulaOrderCache()
    function quartile_func(value, N, quart, timetag, __formula)
        local ret = quartile(value, cache, N, quart, timetag, __formula)
        return ret
    end
    return quartile_func
end
--large 数据集中第 k 个最大值
function c_large()
    local cache = FormulaOrderCache()
    function large_func(value, N, k, timetag, __formula)
        local ret = large(value, cache, N, k, timetag, __formula)
        return ret
    end
    return large_func
end
--small数据集中第 k 个最小值
function c_small()
    local cache = FormulaOrderCache()
    function small_func(value, N, k, timetag, __formula)
        local ret = small(value, cache, N, k, timetag, __formula)
        return ret
    end
    return small_func
end
--skew分布的偏斜度
function c_skew()
    local cache = FormulaDataCacheDouble(cacheDoubleNum)
    function skew_func(value, N, timetag, __formula)
        local ret = skew(value, cache, N, timetag, __formula)
        return ret
    end
    return skew_func
end
--ftest
function c_ftest()
    local cache1 = FormulaDataCacheDouble(cacheDoubleNum)
    local cache2 = FormulaDataCacheDouble(cacheDoubleNum)
    function ftest_func(value1, value2, N, timetag, __formula)
        local ret = ftest(value1, value2, cache1, cache2, N, timetag, __formula)
        return ret
    end
    return ftest_func
end

--数据集的峰值
function c_kurt()
    local cache = FormulaDataCacheDouble(cacheDoubleNum)
    function kurt_func(value, N, timetag, __formula)
        local ret = kurt(value, cache, N, timetag, __formula)
        return ret
    end
    return kurt_func
end
--几何平均值
function c_geomean()
    local cache = FormulaDataCacheDouble(cacheDoubleNum)
    function geomean_func(value, N, timetag, __formula)
        local ret = geomean(value, cache, N, timetag, __formula)
        return ret
    end
    return geomean_func
end
--调和平均值
function c_harmean()
    local cache = FormulaDataCacheDouble(cacheDoubleNum)
    function harmean_func(value, N, timetag, __formula)
        local ret = harmean(value, cache, N, timetag, __formula)
        return ret
    end
    return harmean_func
end

-- INTERCEPT(Y,X,N),求序列Y,X的线性回归线截距
function c_intercept()
    local cache1 = FormulaDataCacheDouble(cacheDoubleNum)
    local cache2 = FormulaDataCacheDouble(cacheDoubleNum)
    function intercept_func(value1, value2, N, timetag, __formula)
        local ret = intercept(value1, value2, cache1, cache2, N, timetag, __formula)
        return ret
    end
    return intercept_func
end
-- RSQ(A,B,N),计算A,B序列的N周期乘积矩相关系数的平方.
function c_rsq()
    local cache1 = FormulaDataCacheDouble(cacheDoubleNum)
    local cache2 = FormulaDataCacheDouble(cacheDoubleNum)
    function rsq_func(value1, value2, N, timetag, __formula)
        local ret = rsq(value1, value2, cache1, cache2, N, timetag, __formula)
        return ret
    end
    return rsq_func
end

-- Pearson（皮尔生）乘积矩相关系数
function c_pearson()
    local cache1 = FormulaDataCacheDouble(cacheDoubleNum)
    local cache2 = FormulaDataCacheDouble(cacheDoubleNum)
    function pearson_func(value1, value2, N, timetag, __formula)
        local ret = pearson(value1, value2, cache1, cache2, N, timetag, __formula)
        return ret
    end
    return pearson_func
end
--通过线性回归法计算每个 x 的 y 预测值时所产生的标准误差
function c_steyx()    
    local cache1 = FormulaDataCacheDouble(cacheDoubleNum)
    local cache2 = FormulaDataCacheDouble(cacheDoubleNum)
    function steyx_func(value1, value2, N, timetag, __formula)
        local ret = steyx(value1, value2, cache1, cache2, N, timetag, __formula)
        return ret
    end
    return steyx_func
end

function c_mode()
    local dcache = FormulaDataCacheDouble(cacheDoubleNum)
    local ccache = FormulaCountCache()
    function mode_func(value, N, timetag, __formula)
        local ret = mode(value, dcache, ccache, N, timetag, __formula)
        return ret
    end
    return mode_func
end

function c_covar()
    local cache1 = FormulaDataCacheDouble(cacheDoubleNum)
    local cache2 = FormulaDataCacheDouble(cacheDoubleNum)
    function covar_func(value1, value2, N, timetag, __formula)
        local ret = covar(value1, value2, cache1, cache2, N, timetag, __formula)
        return ret
    end
    return covar_func
end

function c_beta2()
    local cache1 = FormulaDataCacheDouble(cacheDoubleNum)
    local cache2 = FormulaDataCacheDouble(cacheDoubleNum)
    local cache3 = FormulaDataCacheDouble(cacheDoubleNum)
    function beta2_func(value1, value2, N, timetag, __formula)
        local ret = beta2(value1, value2, cache1, cache2,cache3, N, timetag, __formula)
        return ret
    end
    return beta2_func
end

function c_avedev()
    local cache = FormulaDataCacheDouble(cacheDoubleNum)
    --print("hello")
    function avedev_func(value, N, timetag, __formula)
        local ret = avedev(value, cache, N, timetag, __formula)
        return ret
    end
    return avedev_func
end

function c_devsq()
    local cache = FormulaDataCacheDouble(cacheDoubleNum)
    --print("hello")
    function devsq_func(value, N, timetag, __formula)
        local ret = devsq(value, cache, N, timetag, __formula)
        return ret
    end
    return devsq_func
end

function c_relate()
    local cache1 = FormulaDataCacheDouble(cacheDoubleNum)
    local cache2 = FormulaDataCacheDouble(cacheDoubleNum)
    function relate_func(value1, value2, N, timetag, __formula)
        local ret = relate(value1, value2, cache1, cache2, N, timetag, __formula)
        return ret
    end
    return relate_func
end

function c_std()
    local container = FormulaCacheContainer()
    function wrapper(X, N, timetag, formula)
        return std(container, X, N, timetag, formula)
    end
    return wrapper
end

function c_stdp()
    local container = FormulaCacheContainer()
    function wrapper(X, N, timetag, formula)
        return stdp(container, X, N, timetag, formula)
    end
    return wrapper
end

function c_var()
    local cache = FormulaDataCacheDouble(cacheDoubleNum)
    function var_func(value, N, timetag, __formula)
        local ret = var(value, cache, N, timetag, __formula)
        return ret
    end
    return var_func
end

function c_varp()
    local cache = FormulaDataCacheDouble(cacheDoubleNum)
    function varp_func(value, N, timetag, __formula)
        local ret = varp(value, cache, N, timetag, __formula)
        return ret
    end
    return varp_func
end

function c_std3()
    local history = List.new()
    function std_func(value, N, timetag, __formula)
        List.pushright(history, value);
        local sz = history.last - history.first + 1;
        if (sz > N) then
            List.popleft(history);
            sz = sz - 1
        end
        if (sz < N) then
            return 0 / 0;
        end
        sum1 = 0.0;
        sum2 = 0.0;
        count = 0
        for i = history.first, history.last, 1 do
            temp = history[i];
            sum1 = sum1 + temp;
            sum2 = sum2 + temp * temp;
            count = count + 1;
        end
        sum1 = sum1 * sum1;
        sum1 = sum1 / N;
        ret = math.sqrt((sum2-sum1) / (N-1));
        return ret;
    end
    return std_func;
end


