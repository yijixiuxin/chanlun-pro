------------------------------------------------------------
-- 数学函数
-- 由脚本引擎预先定义，如果有性能问题，可用C++改写
-- @author jiangchanghao
-- @since 2012-9-18
-----------------------------------------------------------


function sgn(val)
    if (type(val) == "boolean")
    then if (val)
        then return 1
        else return 0
        end
    else if (val > 0)
        then return 1
        else if (val == 0)
            then return 0
            else return -1
            end
        end
    end
    return -1
end