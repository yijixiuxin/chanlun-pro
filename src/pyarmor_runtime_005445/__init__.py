# Pyarmor 9.0.6 (pro), 005445, 2024-12-27T14:31:29.880996
def __pyarmor__():
    import platform
    import sys
    from struct import calcsize

    def format_system():
        plat = platform.system().lower()
        plat = ('cygwin' if plat.startswith('cygwin') else
                'linux' if plat.startswith('linux') else
                'freebsd' if plat.startswith(
                    ('freebsd', 'openbsd', 'isilon onefs')) else plat)
        if plat == 'linux':
            if hasattr(sys, 'getandroidapilevel'):
                plat = 'android'
            else:
                cname, cver = platform.libc_ver()
                if cname == 'musl':
                    plat = 'alpine'
                elif cname == 'libc':
                    plat = 'android'
        return plat

    def format_machine():
        mach = platform.machine().lower()
        arch_table = (
            ('x86', ('i386', 'i486', 'i586', 'i686', 'x86')),
            ('x86_64', ('x64', 'x86_64', 'amd64', 'intel')),
            ('arm', ('armv5',)),
            ('armv6', ('armv6l',)),
            ('armv7', ('armv7l',)),
            ('aarch32', ('aarch32',)),
            ('aarch64', ('aarch64', 'arm64')),
            ('ppc64le', ('ppc64le',)),
            ('mips32el', ('mipsel', 'mips32el')),
            ('mips64el', ('mips64el',)),
            ('riscv64', ('riscv64',)),
        )
        for alias, archlist in arch_table:
            if mach in archlist:
                mach = alias
                break
        return mach

    plat, mach = format_system(), format_machine()
    if plat == 'windows' and mach == 'x86_64':
        bitness = calcsize('P'.encode()) * 8
        if bitness == 32:
            mach = 'x86'
    # mach = 'universal' if plat == 'darwin' else mach
    name = '.'.join(['_'.join([plat, mach]), 'pyarmor_runtime'])
    return __import__(name, globals(), locals(), ['__pyarmor__'], level=1)
__pyarmor__ = __pyarmor__().__pyarmor__
