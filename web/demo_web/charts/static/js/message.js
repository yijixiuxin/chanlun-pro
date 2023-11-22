
;(function(global,$){
	/* Created by Luoliang from xi'an, email 821981232@qq.com.
	 * messageLite v1.0
	 * var options={ showClose: true|false, message: '', type:
	 * 'success'|'warning'|'info'|'error'],duration:milliseconds,center:true|false,onClose:cb }
	 */
	 var cssMap={
		'.toast-message':'{position:fixed;left:50%;top:10%;transform:translateX(-50%);border:1px solid #EBEEF5;min-width:380px;transition:top .3s ease-out;opacity .3s ease-out;padding:15px 15px 15px 20px;display:flex;align-items:center;z-index:2000;}',
		'.toast-icon':'{position:relative;margin-right:10px;line-height:1;display:inline-block;-webkit-font-smoothing:antialiased;font-size:14px;font:normal normal normal 14px/1 FontAwesome;text-rendering:auto;}',
		'.toast-message__closeBtn':'{position:absolute;top:50%;right:15px;transform:translateY(-50%);cursor:pointer;color:#C0C4CC;font-size:16px;width:26px;height:26px;line-height:26px;text-align:center;}',
		'.toast-message .toast-message__content':'{padding-right:10px;}',
		'.toast-message__closeBtn:before':'{position:absolute;display:block;left:0;top:0;width:100%;height:100%;content:"×";font-size:16px;}',
		'.toast-message--info':'{border-color:#EBEEF5;background-color:#edf2fc;}',
		'.toast-message--info .toast-icon':'{margin-right:18px;}',
		'.toast-message--info .toast-message__content':'{color:#909399;}',
		'.toast-icon--info:before':'{content:"i";color:#fff;}',
		'.toast-icon--info:after':'{display:block;position:absolute;content:"";width:20px;height:20px;line-height:20px;background:#909399;left:-8px;top:-3px;border-radius:50%;text-align:center;z-index:-1;}',
		'.toast-message--success':'{border-color:#e1f3d8;background-color:#f0f9eb;}',
		'.toast-message--success .toast-icon':'{margin-right:15px;}',
		'.toast-message--success .toast-message__content':'{color:#67C23A;}',
		'.toast-icon--success:before':'{content:"✔";color:#fff;}',
		'.toast-icon--success:after':'{display:block;position:absolute;content:"";width:20px;height:20px;line-height:20px;background:#67C23A;left:-5px;top:-3px;border-radius:50%;text-align:center;z-index:-1;}',
		'.toast-message--warning':'{border-color:#faecd8;background-color:#fdf6ec;}',
		'.toast-message--warning .toast-icon':'{margin-right:16px;}',
		'.toast-message--warning .toast-message__content':'{color:#E6A23C;}',
		'.toast-icon--warning:before':'{content:"!";color:#fff;font-weight:bold;}',
		'.toast-icon--warning:after':'{display:block;position:absolute;content:"";width:20px;height:20px;line-height:20px;background:#E6A23C;left:-8px;top:-3px;border-radius:50%;text-align:center;z-index:-1;}',
		'.toast-message--error':'{border-color:#fde2e2;background-color:#fef0f0;font-weight:bold;}',
		'.toast-message--error .toast-icon':'{margin-right:15px;}',
		'.toast-message--error .toast-message__content':'{color:#F56C6C;}',
		'.toast-icon--error:before':'{content:"×";color:#fff;}',
		'.toast-icon--error:after':'{display:block;position:absolute;content:"";width:20px;height:20px;line-height:20px;background:#F56C6C;left:-5px;top:-3px;border-radius:50%;text-align:center;z-index:-1;}'
	}
	function generateCss(map){
		if(document.querySelector('#message_style')){
			return
		}
		var str='';
		for(var item in map){
			str += item+map[item]
		}
		var st=createEle('style',{attrs:{id:'message_style'},props:{innerHTML:str}});
		document.querySelector('head').appendChild(st);
		updateIECss()
	}
	generateCss(cssMap);
	function createEle(tag,option,children){
		var ele=document.createElement(tag);
		if(option.attrs){
			for(var i in option.attrs){
				ele.setAttribute(i,option.attrs[i])
			}
		}
		if(option.props){
			for(var i in option.props){
				ele[i]=option.props[i]
			}
		}
		
		if(children&&children.length){
			for(var i=0;i<children.length;i++){
				if(children[i]){
					ele.appendChild(children[i])
				}
			}
		}
		return ele
	}
	function closeEle(cb){
		return function(){
			!isIE()?window.event.target.closest(".toast-message").remove():window.event.target.parents(".toast-message").removeNode();
			typeof cb=='function'&&cb();
		}
	}
	function updateIECss(){
		if(isIE()){
			var str='.toast-icon--success:after{left:-5px;}.toast-icon--warning:after{left:-8px;}.toast-icon--error:after{left:-6px;}';
			document.querySelector('#message_style').innerHTML+=str;
		}
	}
	function isIE(){
		return /(MSIE)|(Trident)|(Edge)/.test(navigator.userAgent)
	}
	$.message=function(options){
		if(options){
			options=typeof options=='string'?{type:'info',message:options}:options;
			options.type=options.type||'info';
			var ele=createEle('div',{attrs:{
				class:"toast-message "+(options.type?'toast-message--'+options.type:''),
			}},[options.type?createEle('i',{attrs:{
				class:"toast-icon "+(options.type?'toast-icon--'+options.type:''),
			}},[]):null,createEle('div',{attrs:{
				class:"toast-message__content"
			},props:{innerHTML:options.message}}),options.showClose?createEle('i',{attrs:{
				class:"toast-message__closeBtn"
			},props:{onclick:closeEle(options.onClose)}}):null]);
			var currentEle=document.querySelector('.toast-message');
			if(currentEle){
				!isIE()?currentEle.remove():currentEle.removeNode();
			}
			ele.style.top=options.center?"40%":"0";
			ele.style.opacity=".3";
			document.body.appendChild(ele);
			setTimeout(function(){
				ele.style.top="60px";
				ele.style.opacity="1";
				if(options.center){
					ele.style.top="50%";
					ele.style.transform="translate(-50%)";
				}
			},isIE()?10:5);
			if(options.duration!=0){
				setTimeout(function(){
					!isIE()?(ele&&ele.remove()):(ele&&ele.removeNode())
				},options.duration||5000)
			}		
			
		}
	}
	$.message.version="1.0";
})(this,typeof jQuery!='undefined'?jQuery:this);