// 添加订单记录
$("#add_order_btn").on('click', function (e) {
    e.preventDefault();

    // 清空之前的记录
    $('#add_order_form input').val('');
    $('#add_order_form input[name="market"]').val(market);

    $('#id-date-picker').datetimepicker({
        format: 'YYYY-MM-DD HH:mm:ss',//use this option to display seconds
        icons: {
            time: 'fa fa-clock-o',
            date: 'fa fa-calendar',
            up: 'fa fa-chevron-up',
            down: 'fa fa-chevron-down',
            previous: 'fa fa-chevron-left',
            next: 'fa fa-chevron-right',
            today: 'fa fa-arrows ',
            clear: 'fa fa-trash',
            close: 'fa fa-times'
        }
    });

    $('#add_order_form').find("input[name='code']").val(code);
    var dialog = $("#dialog-message").removeClass('hide').dialog({
        modal: true,
        title: "图表订单记录",
        title_html: true,
        height: 600,
        width: 500,
        buttons: [
            {
                text: "清除代码所有订单",
                class: "btn btn-danger",
                click: function () {
                    let add_order_form = $('#add_order_form').serialize();
                    if (confirm('确认清除当前代码下的所有订单信息？')) {
                        $.ajax({
                            type: "POST",
                            url: "/clean_order",
                            data: add_order_form,
                            dataType: 'json',
                            success: function (result) {
                                if (result['code'] === 200) {
                                    $.message({message: '清除成功，重新加载图表即可', type: 'success'});
                                    dialog.dialog("close");
                                } else {
                                    $.message({message: '清除失败，请检查信息并重试', type: 'error'});
                                }
                            }
                        });
                    }
                }
            },
            {
                text: "取消",
                class: "btn btn-minier",
                click: function () {
                    $(this).dialog("close");
                }
            },
            {
                text: "新增",
                class: "btn btn-primary btn-minier",
                click: function () {
                    let add_order_form = $('#add_order_form').serialize();
                    let form_code = $('#add_order_form input[name="code"]').val();
                    let form_dt = $('#add_order_form input[name="dt"]').val();
                    let form_type = $('#add_order_form select[name="type"]').val();
                    let form_price = $('#add_order_form input[name="price').val();
                    let form_amount = $('#add_order_form input[name="amount"]').val();
                    let form_info = $('#add_order_form input[name="info"]').val();

                    // 增加些简单的验证
                    if (form_code === '') {
                        $.message({message: '代码不能为空', type: 'error'});
                        return false
                    }
                    if (form_dt === '') {
                        $.message({message: '日期不能为空', type: 'error'});
                        return false
                    }
                    if (form_price === '') {
                        $.message({message: '价格不能为空', type: 'error'});
                        return false
                    }
                    if (form_amount === '') {
                        $.message({message: '数量不能为空', type: 'error'});
                        return false
                    }
                    if (form_info === '') {
                        $.message({message: '信息不能为空', type: 'error'});
                        return false
                    }
                    let is_ok = confirm('确认新增以下订单：' +
                        '\n 代码：' + form_code +
                        '\n 时间：' + form_dt +
                        '\n 操作：' + form_type +
                        '\n 价格：' + form_price +
                        '\n 数量：' + form_amount +
                        '\n 信息：' + form_info
                    );
                    if (is_ok) {
                        $.ajax({
                            type: "POST",
                            url: "/add_order",
                            data: add_order_form,
                            dataType: 'json',
                            success: function (result) {
                                if (result['code'] === 200) {
                                    $.message({message: '订单记录保存成功，重新加载图表即可展示', type: 'success'});
                                    dialog.dialog("close");
                                } else {
                                    $.message({message: '保存失败，请检查信息并重试', type: 'error'});
                                }
                            }
                        });
                    }
                }
            }
        ]
    });
});
