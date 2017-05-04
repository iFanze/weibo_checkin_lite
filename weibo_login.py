# -*- coding: utf-8 -*-

__version__ = '1.0.0'
__author__ = 'Meng Fanze (iFanze@outlook.com)'

import re
import urllib.parse
import urllib.request
import http.cookiejar
import base64
import binascii
import rsa
from weibo import _parse_json

# 为所有请求添加cookie
cj = http.cookiejar.LWPCookieJar()
cookie_support = urllib.request.HTTPCookieProcessor(cj)
opener = urllib.request.build_opener(cookie_support, urllib.request.HTTPHandler)
urllib.request.install_opener(opener)


# 调试方法
def _print_obj(obj):
    itemdir = obj.__dict__
    print('--------obj viewer--------')
    for i in itemdir:
        print('%s : %s' % (i, itemdir[i]))
    print('------obj viewer end------')


class WeiboLoginError(BaseException):
    """raise WeiboLoginError if caused error"""

    def __init__(self, error_code, error):
        self.error_code = error_code
        self.error = error
        BaseException.__init__(self, error)

    def __str__(self):
        return 'WeiboLoginError: %s: %s' % (self.error_code, self.error)


class WeiboLogin:
    def __init__(self, username, password, client_id, redirect_url):
        self.username = username
        self.password = password
        self.client_id = client_id
        self.redirect_url = redirect_url

    def get_code(self):
        # 预登陆
        prelogin_url = ('http://login.sina.com.cn/sso/prelogin.php?'
                        'entry=openapi&callback=sinaSSOController.preloginCallBack&su=%s&'
                        'rsakt=mod&checkpin=1&client=ssologin.js(v1.4.15)&_=1400822309846') % self.username
        prelogin_req = urllib.request.Request(prelogin_url)
        prelogin_res = urllib.request.urlopen(prelogin_req)
        prelogin_res_text = prelogin_res.read().decode('utf-8')

        # 获取预登陆得到的参数
        servertime = re.findall('"servertime":(.*?),', prelogin_res_text)[0]
        pubkey = re.findall('"pubkey":"(.*?)",', prelogin_res_text)[0]
        rsakv = re.findall('"rsakv":"(.*?)",', prelogin_res_text)[0]
        nonce = re.findall('"nonce":"(.*?)",', prelogin_res_text)[0]

        # 数据加密
        su = base64.b64encode(bytes(urllib.request.quote(self.username), encoding='utf-8'))
        pubkey = int(pubkey, 16)
        key = rsa.PublicKey(pubkey, 65537)
        message = bytes(str(servertime) + '\t' + str(nonce) + '\n' + str(self.password), encoding='utf-8')
        sp = binascii.b2a_hex(rsa.encrypt(message, key))

        # 构建Post参数
        login_param = {
            'entry': 'openapi',
            'gateway': 1,
            'from': '',
            'savestate': 0,
            'useticket': 1,
            'pagerefer': '',
            'ct': 1800,
            's': 1,
            'vsnf': 1,
            'vsnval': '',
            'door': '',
            'su': su,
            'cdult': 2,
            'returntype': 'TEXT',
            'service': 'miniblog',
            'servertime': servertime,
            'nonce': nonce,
            'pwencode': 'rsa2',
            'rsakv': rsakv,
            'sp': sp,
            'sr': '1680*1050',
            'encoding': 'UTF-8',
            'prelt': 961
        }
        login_param = urllib.parse.urlencode(login_param).encode('utf-8')

        # 登录
        login_url = 'http://login.sina.com.cn/sso/login.php?client=ssologin.js(v1.4.15)'
        login_headers = {'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)'}
        login_req = urllib.request.Request(login_url, login_param, login_headers)
        login_res = urllib.request.urlopen(login_req)
        login_res_text = login_res.read().decode('gbk')
        login_res_json = _parse_json(login_res_text)

        if not login_res_json.retcode == '0':
            raise WeiboLoginError(login_res_json.retcode, login_res_json.reason)
        else:
            # 认证，获取code
            auth_url = "https://api.weibo.com/oauth2/authorize"
            auth_headers = {
                'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
                'Referer': 'https://api.weibo.com/oauth2/authorize'
            }
            auth_param = {
                'client_id': self.client_id,
                'redirect_uri': self.redirect_url,
                'response_type': 'code',
                'ticket': login_res_json.ticket,
                'action': 'login'
            }
            auth_param = urllib.parse.urlencode(auth_param).encode('utf-8')
            auth_req = urllib.request.Request(auth_url, auth_param, auth_headers)
            auth_res = urllib.request.urlopen(auth_req)
            code = re.findall('code=(.*)', auth_res.url)[0]
            return code


if __name__ == "__main__":
    task = WeiboLogin('ichen0201@sina.com', 'xxxxxx', '3226611318', 'https://api.weibo.com/oauth2/default.html')
    try:
        res = task.get_code()
        print("Login Success, code = %s" % res)
    except WeiboLoginError as e:
        print("Login Fail [%s]: %s" % (e.error_code, e.error))
