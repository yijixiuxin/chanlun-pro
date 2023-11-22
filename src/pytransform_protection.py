# platforms: windows.x86.0,windows.x86_64.0,linux.x86_64.0,darwin.x86_64.0,darwin.aarch64.0,linux.aarch32.0,linux.aarch64.0
# advanced: 0
# suffix: 
# license: outer
def protect_pytransform():

    import pytransform as pytransform

    def assert_builtin(func):
        type = ''.__class__.__class__
        builtin_function = type(''.join)
        if type(func) is not builtin_function:
            raise RuntimeError('%s() is not a builtin' % func.__name__)

    def check_obfuscated_script():
        CO_SIZES = 55, 52, 49, 46, 42, 40, 38, 36
        CO_NAMES = set(['pytransform', 'pyarmor_runtime',
                        '__pyarmor__', '__name__', '__file__'])
        co = pytransform.sys._getframe(3).f_code
        if not ((set(co.co_names) <= CO_NAMES)
                and (len(co.co_code) in CO_SIZES)):
            raise RuntimeError('unexpected obfuscated script')

    def check_mod_pytransform():
        def _check_co_key(co, v):
            return (len(co.co_names), len(co.co_consts), len(co.co_code)) == v
        for k, (v1, v2, v3) in [('dllmethod', ((0, 3, 16), (0, 1, 14), None)), ('init_pytransform', ((0, 1, 14), None, None)), ('init_runtime', ((0, 1, 14), None, None)), ('_load_library', ((33, 22, 666), None, None)), ('get_registration_code', ((0, 1, 14), None, None)), ('get_expired_days', ((0, 1, 14), None, None)), ('get_hd_info', ((12, 10, 124), None, None)), ('get_license_info', ((11, 24, 394), None, None)), ('get_license_code', ((1, 2, 10), None, None)), ('format_platform', ((18, 18, 278), None, None)), ('pyarmor_init', ((3, 1, 22), None, None)), ('pyarmor_runtime', ((12, 5, 130), None, None)), ('_match_features', ((1, 2, 30), None, None))]:
            co = getattr(pytransform, k).__code__
            if not _check_co_key(co, v1):
                raise RuntimeError('unexpected pytransform.py')
            if v2:
                if not _check_co_key(co.co_consts[1], v2):
                    raise RuntimeError('unexpected pytransform.py')
            if v3:
                if not _check_co_key(co.__closure__[0].cell_contents.__code__, v3):
                    raise RuntimeError('unexpected pytransform.py')

    def check_lib_pytransform():
        platname = pytransform.sys.platform
        if platname.startswith('darwin'):
            return
        libname = '_pytransform.dylib' if platname.startswith('darwin') else \
                  '_pytransform.dll' if platname.startswith('win') else \
                  '_pytransform.dll' if platname.startswith('cygwin') else \
                  '_pytransform.so'
        if getattr(pytransform.sys, 'frozen', False):
            filename = pytransform.os.path.join(pytransform.sys._MEIPASS, libname)
        else:
            filename = pytransform.os.path.join(pytransform.os.path.dirname(pytransform.__file__), pytransform.os.path.join(pytransform.plat_path, pytransform.format_platform()), libname)

        with open(filename, 'rb') as f:
            buf = bytearray(f.read())
        value = sum(buf)
        if getattr(pytransform.sys, 'frozen', False) and sys.platform == 'darwin':
            if '':
                value += 886 - sum(b'') + 299
            else:
                value += 1217

        if value not in [70790242, 50349072, 55204070, 70510627, 47203504, 90488388, 56366727]:
            raise RuntimeError('unexpected %s' % filename)

    assert_builtin(sum)
    assert_builtin(open)
    assert_builtin(len)

    check_obfuscated_script()
    check_mod_pytransform()
    check_lib_pytransform()


protect_pytransform()
