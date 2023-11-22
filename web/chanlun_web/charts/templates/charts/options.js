// 周期与配置的选择

// 查询缠论配置
function query_cl_chart_config(market, code) {
    $('#ace-settings-box').load("/cl_chart_config?market=" + market + "&code=" + code, {}, function () {
        $('#zs_bi_type').chosen({allow_single_deselect: true});
        $('#zs_xd_type').chosen({allow_single_deselect: true});
        $('.chosen-container-multi').attr('style', 'width:100%;');
    });
}

// 保存缠论配置
function save_cl_chart_config(market, code, is_del) {
    var cl_chart_config = {
        'market': market, 'code': code, 'is_del': is_del
    }
    $.each($('.chart_setting input,.chart_setting select'), function (i, obj) {
        var _key = $(obj).attr('id');
        var _type = $(obj).attr('type');
        var _val = $(this).val();
        if (_type === 'checkbox') {
            _val = $(this).is(':checked') ? '1' : '0';
        }
        cl_chart_config[_key] = _val
    });
    $.ajax({
        type: "POST",
        url: "/cl_chart_config_save",
        data: cl_chart_config,
        dataType: 'json',
        traditional: true,
        success: function (result) {
            if (result['code'] === 200) {
                if (is_del) {
                    query_cl_chart_config(market, code);
                }
            } else {
                $.message({message: '缠论和图表配置项保存错误', type: 'error'});
            }
        }
    });
}

$("body").delegate('.chart_setting input,.chart_setting select', 'change', function () {
    let _key = $(this).attr('id');
    let _val = $(this).val();
    console.log('setting change key ' + _key);
    if (_key === 'config_use_type' && _val === 'common') {
        if (confirm('确定要将目前的独立配置，恢复为通用配置吗？')) {
            save_cl_chart_config(market, code, true);
            return false;
        } else {
            return false;
        }
    }
    save_cl_chart_config(market, code, false);

    $.message({
        message: '缠论配置变更，需重新点击当前周期更新图表数据', type: 'success'
    });
});

