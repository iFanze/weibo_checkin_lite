config = {
    "worker_id": 1,
    "redis_config": {
        "host": "localhost",
        "port": 6379,
        "db": 0
    },
    "mysql_config": {
        "host": "localhost",
        "port": 3306,
        "db": "weibo_checkin",
        "username": "root",
        "password": "admin"
    },
    "weibo_apps": [{
        "name": "Fanze1",
        # "app_key": "3226611318",
        # "app_secret": "4f94b19d1d30c6bce2505e69d22cd62e",
        "app_key": "1617636062",
        "app_secret": "3bc936c29c951ff20354717342d11ff4",
        "callback_url": "https://api.weibo.com/oauth2/default.html",
        "accounts": [{
            "username": "ichen0201@sina.com",
            "password": "s2013h1cfr",
            # "username": "15827366706",
            # "password": "s2010h1cfr"
        }, {
            "username": "",
            "password": ""
        }]
    }, {
        "name": "Fanze2",
        "app_key": "1617636062",
        "app_secret": "3bc936c29c951ff20354717342d11ff4",
        "callback_url": "https://api.weibo.com/oauth2/default.html",
        "accounts": [{
            "username": "ichen0201@sina.com",
            "password": "s2013h1cfr"
        }, {
            "username": "",
            "password": ""
        }]
    }],
}
