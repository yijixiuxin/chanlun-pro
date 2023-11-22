// 请求 Kline 方法
function fetchKlinesData(chart_obj, market, code, frequency, update = false) {
    $('#loading').loading({theme: 'dark'});

    let market_klines_urls = {
        'a': '/stock/kline',
        'currency': '/currency/kline',
        'futures': '/futures/kline',
        'hk': '/hk/kline',
        'us': '/us/kline',
    };

    let post_data = {
        'code': code, 'frequency': frequency,
    }
    $.ajax({
        type: "POST", url: market_klines_urls[market], data: post_data, success: function (result) {
            var re_obj = (new Function("return " + result))();
            if (update === false) {
                chart_obj.clear();
            }
            chart_obj.setOption(re_obj);
            $('#loading').loading('stop');
        }
    });
}


// 大图小图展示
function chart_show_height(_type) {
    if (_type === '0') {
        $('#kline_high').css('height', chart_div_high / 2);
        $('#kline_low').css('height', chart_div_high / 2);
    } else {
        $('#kline_high').css('height', chart_div_high);
        $('#kline_low').css('height', chart_div_high);
    }
    chart_high.resize();
    chart_low.resize();
}

// 机会列表的展示
function jhs_list_show() {
    var urls = {
        'a': '/stock/jhs', 'currency': '/currency/jhs', 'hk': '/hk/jhs', 'us': '/us/jhs', 'futures': '/futures/jhs',
    }
    $.ajax({
        type: "GET", url: urls[market], dataType: 'json', success: function (result) {
            if (result['code'] === 200) {
                $('#jhs_ul').html('');
                for (let i = 0; i < result['data'].length; i++) {
                    jh = result['data'][i];
                    $('#jhs_ul').append('<li class="list-group-item"><p class="list-group-item-heading"><a href="javascript:void(0);" class="code" data-code="' + jh['code'] + '" data-name="' + jh['name'] + '">' + jh['name'] + '</a> <span>' + jh['frequency'] + '</span></p> <p class="list-group-item-text">' + jh['jh_type'] + ' <br/>' + jh['is_done'] + ' ' + jh['is_td'] + '<br/> ' + jh['datetime_str'] + '</p></li>');
                }
            }
        }
    });
}

// 获取股票行业与概念信息
function stock_plate(code) {
    $.ajax({
        type: "GET",
        url: "/stock/plate?code=" + code,
        dataType: 'json',
        success: function (result) {
            if (result['code'] === 200) {
                hy_list = '';
                gn_list = '';
                for (let i = 0; i < result['data']['HY'].length; i++) {
                    hy = result['data']['HY'][i];
                    hy_list += '<a href="javascript:void(0);" class="plate" data-hycode="' + hy['code'] + '">' + hy['name'] + '</a>  / '
                }
                for (let i = 0; i < result['data']['GN'].length; i++) {
                    gn = result['data']['GN'][i];
                    gn_list += '<a href="javascript:void(0);" class="plate" data-hycode="' + gn['code'] + '">' + gn['name'] + '</a>  / '
                }
                $('.hy_list').html(hy_list);
                $('.gn_list').html(gn_list);
            }
        }
    });
    return true
}

function stock_update_rates() {
//    if (market === 'us') {
//        return true;
//    }
    // 获取自选列表中的代码
    let codes = [];
    $('#my_stocks').find('.code').each(function () {
        codes.push($(this).data('code'))
    });
    $.ajax({
        type: "POST",
        url: "/ticks",
        data: {'market': market, 'codes': JSON.stringify(codes)},
        dataType: 'json',
        success: function (result) {
            if (result['code'] === 200) {
                for (let i = 0; i < result['data']['ticks'].length; i++) {
                    let tick = result['data']['ticks'][i];
                    let color = tick['rate'] > 0 ? 'red' : 'green';
                    let obj_span_rate = $('#my_stocks .code[data-code="' + tick['code'] + '"]').find('.menu-rate');
                    obj_span_rate.html(tick['rate'] + '%');
                    obj_span_rate.css('color', color);
                }
                let now_trading = result['data']['now_trading'];
                if (now_trading !== true) {
                    clearInterval(interval_update_rates);
                }
            }
        }
    });
}

// 根据代码，查询自选分组
function zixuan_code_query_zx_names(market, code) {
    $.ajax({
        type: "GET",
        url: "/zixuan/code_zx_names?market_type=" + market + "&code=" + code,
        dataType: 'json',
        success: function (result) {
            if (result['code'] === 200) {
                $('#zixuan_zx_names').html('');
                for (let i = 0; i < result['data'].length; i++) {
                    var zx_name = result['data'][i];
                    var checked = zx_name['exists'] === 1 ? 'checked' : '';
                    $('#zixuan_zx_names').append(
                        '<li><div class="checkbox"><label>' + '' +
                        '<input name="zx_name" type="checkbox" data-zx-name="' + zx_name['zx_name'] + '" data-market="' + market + '" data-code="' + code + '" ' + checked + ' class="ace opt_zx">' +
                        '<span class="lbl">' + zx_name['zx_name'] + '</span>' +
                        '</label></div></li>'
                    );
                }
            }
        }
    });
}

// 自选操作
$("body").delegate('.opt_zx', 'change', function () {
    var checked = $(this).prop('checked');
    var zx_name = $(this).data('zx-name');
    var market = $(this).data('market');
    var code = $(this).data('code');
    var opt = checked === true ? 'add' : 'del';
    $.ajax({
        type: "GET",
        url: '/zixuan/opt?market_type=' + market + '&zx_name=' + zx_name + '&opt=' + opt + '&code=' + code + '&name=',
        dataType: 'json',
        success: function (result) {
            let msg = '';
            if (opt === 'add') {
                msg = '添加自选组 ' + zx_name + ' 成功';
            } else {
                msg = '删除自选组 ' + zx_name + ' 成功';
            }
            if (result['code'] === 200) {
                $.message({message: msg, type: 'success'});
            } else {
                $.message({message: '操作自选失败', type: 'error'});
            }
        }
    });
});
