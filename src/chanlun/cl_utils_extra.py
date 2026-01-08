
def check_loose_3mmd(cl_instance, line, zs, mmd_type):
    """
    检查宽松的第三类买卖点
    允许瞬间击穿中枢（如单根K线影线击穿，或极少数K线击穿）
    """
    if mmd_type == "3buy":
        # 严格条件：low > zg
        if line.low > zs.zg:
            return True
        
        # 宽松条件：
        # 1. 击穿幅度不大（比如不超过中枢高度的 1%? 这里先不设幅度限制，主要看形态）
        # 2. 击穿时间极短（比如只有1-2根K线）
        # 3. 收盘价必须在线上方（或者大部分收盘价在线上方）
        
        # 获取线对应的K线序列
        klines = cl_instance.cl_klines[line.start.k.index : line.end.k.index + 1]
        if not klines:
            return False
            
        # 检查击穿ZG的K线数量
        break_count = 0
        for k in klines:
            if k.l < zs.zg:
                break_count += 1
                # 如果收盘价也击穿，且击穿较深，则认为无效
                # 这里简单判定：如果收盘价也在下面，算更严重的击穿
                if k.c < zs.zg:
                    break_count += 1 # 加重权重
        
        # 允许最多 2 个单位的击穿（例如1根K线收盘破+影线破 = 2，或者2根影线破）
        if break_count <= 2:
            return True
            
    elif mmd_type == "3sell":
        # 严格条件：high < zd
        if line.high < zs.zd:
            return True
            
        klines = cl_instance.cl_klines[line.start.k.index : line.end.k.index + 1]
        if not klines:
            return False
            
        break_count = 0
        for k in klines:
            if k.h > zs.zd:
                break_count += 1
                if k.c > zs.zd:
                    break_count += 1
        
        if break_count <= 2:
            return True
            
    return False
