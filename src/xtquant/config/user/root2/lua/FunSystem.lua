------------------------------------------------------------
-- 系统函数
-- 由脚本引擎预先定义，如果有性能问题，可用C++改写
-- @author jiangchanghao
-- @since 2012-10-11
-----------------------------------------------------------

function c_print()
    local pc = PrintCache()
    function printout(...)
        local tt = {...}
        local len = #tt
        timetag = tt[len - 1]
        formula = tt[len]
        if (len == 3 and type(tt[1]) == "nil") then
            printOut(pc, formula)
            return 
        end
        local i = 1
        repeat
            local v = tt[i]
            if (type(v) == "string") then 
                printStr(v, tt[i+1], pc, timetag, formula)
            else 
                if (type(v) == "boolean") then
                    printBool(v, tt[i+1], pc, timetag, formula)
                else
                    printNum(v, tt[i+1], pc, timetag, formula)
                end
            end
            i = i + 2
        until i >= len - 1
    end    
    return printout
end

function c_serialize()
    local cc = FormulaCacheContainer()
    function ff(...)
        local tt = {...}
        local len = #tt
        
        timetag = tt[len - 1]
        formula = tt[len]
        
        serialize_c(-1, 0, 0, cc, timetag, formula)
        
        if len == 3 then
            serialize_c(0, 0, tt[1], cc, timetag, formula)
            return serialize_c(1, 0, 0, cc, timetag, formula)
        end
        
        if tt[1] == 'list' then
            for i = 2, len - 2, 1 do
                serialize_c(0, i - 2, tt[i], cc, timetag, formula)
            end
            return '[' .. serialize_c(1, 0, 0, cc, timetag, formula) .. ']'
        end
        
        if tt[1] == 'dict' then
            for i = 2, len - 2, 2 do
                if i + 1 <= len - 2 then
                    serialize_c(0, tt[i], tt[i + 1], cc, timetag, formula)
                end
            end
            return '{' .. serialize_c(1, 0, 0, cc, timetag, formula) .. '}'
        end
        
        for i = 2, len - 2, 1 do
            serialize_c(0, i - 2, tt[i], cc, timetag, formula)
        end
        return serialize_c(1, 0, 0, cc, timetag, formula)
    end
    return ff
end
