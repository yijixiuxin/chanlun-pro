// 定时刷新
$('#shuaxin').click(function () {
    let val = $(this).attr('value');
    if (val === '0') {
        // 开启自动更新
        $(this).attr('value', '1');
        $(this).text('关闭自动更新');
        intervalId = setInterval(function () {
            fetchKlinesData(chart_high, market, code, frequency_high, true);
            fetchKlinesData(chart_low, market, code, frequency_low, true);
        }, 15000);
    }
    if (val === '1') {
        // 关闭自动更新
        $(this).attr('value', '0');
        $(this).text('开启自动更新');
        clearInterval(intervalId);
    }
});

$('#show_datu').click(function () {
    let val = $(this).attr('value');
    if (val === '0') {
        $.cookie(cookie_pre + '_show_datu', '0')
        $(this).attr('value', '1');
        $(this).text('切换到大图');
        chart_show_height(val)
    } else {
        $.cookie(cookie_pre + '_show_datu', '1')
        $(this).attr('value', '0');
        $(this).text('切换到小图');
        chart_show_height(val)
    }
});
if ($.cookie(cookie_pre + '_show_datu') === '1') {
    $('#show_datu').attr('value', '0');
    $('#show_datu').text('切换到小图');
    chart_show_height('1')
} else {
    $('#show_datu').attr('value', '1');
    $('#show_datu').text('切换到大图');
    chart_show_height('0')
}

// 周期切换功能
if ($.cookie(cookie_pre + '_frequency_high') !== undefined) {
    frequency_high = $.cookie(cookie_pre + '_frequency_high');
}
$('#zq_high').find('[data-zq="' + frequency_high + '"]').addClass('btn-primary');

$('#zq_high button').click(function () {
    $('#zq_high button').removeClass('btn-primary');
    $(this).addClass('btn-primary');
    frequency_high = $(this).attr('data-zq');
    $.cookie(cookie_pre + '_frequency_high', frequency_high, {expires: 999});
    fetchKlinesData(chart_high, market, code, frequency_high, false);
});

if ($.cookie(cookie_pre + '_frequency_low') !== undefined) {
    frequency_low = $.cookie(cookie_pre + '_frequency_low');
}
$('#zq_low').find('[data-zq="' + frequency_low + '"]').addClass('btn-primary');

$('#zq_low button').click(function () {
    $('#zq_low button').removeClass('btn-primary');
    $(this).addClass('btn-primary');
    frequency_low = $(this).attr('data-zq');
    $.cookie(cookie_pre + '_frequency_low', frequency_low, {expires: 999});
    fetchKlinesData(chart_low, market, code, frequency_low, false);
});


// 代码搜索
$('#search_code').typeahead(null, {
    name: 'search_code', display: 'code', minLength: 2, limit: 20, highlight: true, source: new Bloodhound({
        datumTokenizer: Bloodhound.tokenizers.obj.whitespace('value'),
        queryTokenizer: Bloodhound.tokenizers.whitespace,
        remote: {
            url: '/search_code?market=' + market + '&query=%QUERY', wildcard: '%QUERY'
        }
    }), templates: {
        empty: ['<div class="empty-message">', '无代码信息', '</div>'].join('\n'), suggestion: function (res_json) {
            if (market === 'currency') {
                return '<div style="font-size:14px;"><b>' + res_json['code'] + '</b></div>'
            } else {
                return '<div style="font-size:14px;"><b>' + res_json['code'] + '</b>/' + res_json['name'] + '</div>'
            }
        },
    }
});

// 搜索框点击展示
$('#stock_ok').click(function () {
    var stock_code = $('#search_code').val();
    var stock_name = $('#search_code').attr('data-name');
    if (stock_code !== '') {
        code = stock_code;
        name = stock_name;
        fetchKlinesData(chart_high, market, code, frequency_high, false);
        fetchKlinesData(chart_low, market, code, frequency_low, false);
        query_cl_chart_config(market, code);
        if (market === 'a' || market === 'hk') {
            stock_plate(code);
        }
        zixuan_code_query_zx_names(market, code);
    }
});

// 点击切换股票行情
$("body").delegate('.code', 'click', function () {
    $('#my_stocks li').removeClass('active');
    $(this).addClass('active');
    code = $(this).attr('data-code');
    name = $(this).attr('data-name');
    fetchKlinesData(chart_high, market, code, frequency_high, false);
    fetchKlinesData(chart_low, market, code, frequency_low, false);
    query_cl_chart_config(market, code);
    if (market === 'a' || market === 'hk') {
        stock_plate(code);
    }
    zixuan_code_query_zx_names(market, code);
});

// 自选切换
$('.btn_zixuan').click(function () {
    $('.btn_zixuan').removeClass('btn-success');
    $(this).addClass('btn-success');
    var zx_name = $(this).attr('data-zxname');
    $.ajax({
        type: "GET",
        url: "/zixuan/stocks?market_type=" + market + "&zx_name=" + zx_name,
        dataType: 'json',
        success: function (result) {
            if (result['code'] === 200) {
                $('#my_stocks').html('');
                for (var i = 0; i < result['data'].length; i++) {
                    var stock = result['data'][i];
                    if (market === 'currency') {
                        $('#my_stocks').append('<li class="code" data-code="' + stock['code'] + '" data-name="' + stock['name'] + '"><a href="#' + stock['code'] + '"><span class="menu-text">' + stock['code'] + '</span><span class="menu-rate">--</span></a></li>');
                    } else {
                        $('#my_stocks').append('<li class="code" data-code="' + stock['code'] + '" data-name="' + stock['name'] + '"><a href="#' + stock['code'] + '"><span class="menu-text">' + stock['code'] + ' / ' + stock['name'] + '</span><span class="menu-rate">--</span></a></li>');
                    }
                }
                $('#stock_search').quicksearch('#my_stocks li');
                stock_update_rates();
            }
        }
    });
});

// 自选列表的涨跌幅获取
let interval_update_rates = undefined;


// 每60s更新涨跌幅
interval_update_rates = setInterval(stock_update_rates, 60000);


