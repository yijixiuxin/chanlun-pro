
class FileLock:
    def __init__(this, path, auto_lock = False):
        this.path = path
        this.fhandle = None
        if auto_lock:
            this.lock()
        return

    def is_lock(this):
        import os
        if os.path.exists(this.path):
            try:
                os.remove(this.path)
                return False
            except Exception as e:
                return True
        return False

    def lock(this):
        if this.fhandle:
            raise this.fhandle
        try:
            this.fhandle = open(this.path, 'w')
        except Exception as e:
            return False
        return True

    def unlock(this):
        if not this.fhandle:
            raise this.fhandle
        this.fhandle.close()
        this.fhandle = None
        return True

    def clean(this):
        import os
        if not os.path.exists(this.path):
            return True
        try:
            if os.path.isfile(this.path):
                os.remove(this.path)
                return True
        except Exception as e:
            pass
        return False

class Extender:
    from ctypes import c_float, c_short
    value_type = c_float
    rank_type = c_short

    def __init__(self, base_dir):
        import os
        self.base_dir = os.path.join(base_dir, 'EP')

    def read_config(self):
        import json, os
        data = None
        with open(os.path.join(self.file, 'config'), 'r', encoding='utf-8') as f:
            data = json.loads(f.read())

        if data:
            self.stocklist = []
            for i in range(1, len(data['stocklist']), 2):
                for stock in data['stocklist'][i]:
                    self.stocklist.append("%s.%s" % (stock, data['stocklist'][i - 1]))

            self.timedatelist = data['tradedatelist']

    def read_data(self, data, time_indexs, stock_length):
        from ctypes import c_float, c_short, sizeof, cast, POINTER
        res = {}
        num = (sizeof(self.value_type) + sizeof(self.rank_type)) * stock_length
        for time_index in time_indexs:
            index = num * time_index
            value_data = data[index: index + sizeof(self.value_type) * stock_length]
            values = cast(value_data, POINTER(c_float))
            rank_data = data[index + sizeof(self.value_type) * stock_length: index + num]
            ranks = cast(rank_data, POINTER(c_short))
            res[self.timedatelist[time_index]] = [(round(values[i], 3), ranks[i]) for i in range(stock_length)]

        return res

    def format_time(self, times):
        import time
        if type(times) == str:
            return int(time.mktime(time.strptime(times, '%Y%m%d'))) * 1000
        elif type(times) == int:
            if times < 0:
                return self.timedatelist[times]
            elif times < ((1 << 31) - 1):
                return times * 1000
            else:
                return times

    def show_extend_data(self, file, times):
        import time, os
        self.file = os.path.join(self.base_dir, file + '_Xdat')
        if not os.path.isdir(self.file):
            return "No such file"

        fs = FileLock(os.path.join(self.file, 'filelock'), False)

        while fs.is_lock():
            print('文件被占用')
            time.sleep(1)
        fs.lock()

        self.read_config()

        time_list = []

        if not times:
            time_list = self.timedatelist
        elif type(times) == list:
            time_list.extend([self.format_time(i) for i in times])
        else:
            time_list.append(self.format_time(times))


        time_index = [self.timedatelist.index(time) for time in time_list if self.timedatelist.count(time) != 0]

        stock_length = len(self.stocklist)
        data = None
        with open(os.path.join(self.file, 'data'), 'rb') as f:
            data = f.read()
        fs.unlock()
        res = self.read_data(data, time_index, stock_length)
        return self.stocklist, res


def show_extend_data(file, times):
    import os
    from . import xtdata as xd
    exd = Extender(os.path.join(xd.init_data_dir(), '..', 'datadir'))

    return exd.show_extend_data(file, times)
