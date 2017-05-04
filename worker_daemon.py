import logging
import sys
import os
import time

from worker_config import config

from weibo_login import WeiboLoginError, WeiboLogin
from weibo import APIError, APIClient

from daemon import Daemon
from mysql_conn import MySQLConn
from redis_conn import RedisConn

from MySQLdb import OperationalError


class JsonDict(dict):
    """ general json object that allows attributes to be bound to and also behaves like a dict """

    def __getattr__(self, attr):
        try:
            return self[attr]
        except KeyError:
            raise AttributeError(r"'JsonDict' object has no attribute '%s'" % attr)

    def __setattr__(self, attr, value):
        self[attr] = value

    def __init__(self, dictionary):
        keys = list(dictionary.keys())
        for key in keys:
            self.__setattr__(key, dictionary[key])


class WorkerDaemon(Daemon, MySQLConn, RedisConn):
    def __init__(self, pidfile, workerid, stdin=os.devnull, stdout=os.devnull, stderr=os.devnull):
        # 初始化Daemon
        super(WorkerDaemon, self).__init__(pidfile, stdin, stdout, stderr)
        # 初始化MySQLConn
        super(Daemon, self).__init__(config["mysql_config"]["host"],
                                     config["mysql_config"]["port"],
                                     config["mysql_config"]["db"],
                                     config["mysql_config"]["username"],
                                     config["mysql_config"]["password"])
        # 初始化Redis
        super(MySQLConn, self).__init__(config["redis_config"]["host"],
                                        config["redis_config"]["port"],
                                        config["redis_config"]["db"], )

        self.worker_id = workerid
        self.weibo_apps = []
        self.delta_latlon = 0.001
        # self.mysql_conn = MySQLdb.connect(host="localhost", user="root",
        #                                   passwd="admin", db="weibo_checkin")
        # self.redis_conn = redis.Redis(host='localhost', port=6379, db=0,
        #                              decode_responses=True)
        self.doing_list = []
        self.weibo_client = None
        logging.info("Worker #%s inited." % workerid)

    def run(self):
        """ 运行worker """

        self.get_weibo_token(config["weibo_apps"][0]["app_key"],
                             config["weibo_apps"][0]["app_secret"],
                             config["weibo_apps"][0]["callback_url"],
                             config["weibo_apps"][0]["accounts"][0]["username"],
                             config["weibo_apps"][0]["accounts"][0]["password"],
                             )

        # 如果程序意外退出，需要继续处理仍留在doing_list中的任务。
        last_doing = self.redis_conn.lrange("poi_worker_" + str(self.worker_id) + "_doing_list", 0, -1)
        last_doing = list(map(int, last_doing))
        for doing in last_doing:
            logging.info("poi last_doing found, taskid: %s" % doing)
            self.execute_poi_task(doing)

        # 监视新任务。
        while True:
            sys.stdout.write('.')
            sys.stdout.flush()

            self.mysql_conn.ping(True)

            # 弹出poi_worker_1_todo_list。
            todo = self.redis_conn.lpop("poi_worker_" + str(self.worker_id) + "_todo_list")
            if todo:
                logging.info("poi_todo found, taskid: %s" % todo)
                self.redis_conn.rpush("poi_worker_" + str(self.worker_id) + "_doing_list", todo)
                self.doing_list.append(todo)
                self.execute_poi_task(todo)

            time.sleep(2)

    def read_weibo_apps(self, _config):
        self.weibo_apps = _config

    def get_poi_task_x_worker_self(self, taskid):
        task = self.redis_conn.hgetall("poi_task_" + str(taskid) + "_worker_" + str(self.worker_id))
        task = JsonDict(task)
        task.cur_lat = float(task.cur_lat)
        task.cur_lon = float(task.cur_lon)
        task.max_lon = float(task.max_lon)
        task.max_lat = float(task.max_lat)
        task.min_lon = float(task.min_lon)
        task.min_lat = float(task.min_lat)
        task.progress = int(task.progress)
        return task

    def get_weibo_token(self, appkey, appsecret, url, username, password):
        logging.info("preparing weibo OAuth2:")
        logging.info("appkey: %s username: %s" % (appkey, username))
        self.weibo_client = APIClient(app_key=appkey, app_secret=appsecret, redirect_uri=url)
        code = WeiboLogin(username, password, appkey, url).get_code()
        logging.info("code: %s" % code)
        r = self.weibo_client.request_access_token(code)
        self.weibo_client.set_access_token(r.access_token, r.expires_in)
        logging.info("token: %s" % r.access_token)

    def save_poi(self, poi, taskid):
        sql = "SELECT `area_id` from `weibo_checkin_poitask` where `id` = ?"
        res = self.mysql_select(sql, (taskid,), 1, log=False)
        areaid = int(res["area_id"])
        sql = "SELECT `task_id` from `weibo_checkin_poi` where `poiid` = ?"
        res = self.mysql_select(sql, (poi["poiid"],), log=False)
        if len(res) != 0:
            return False
        sql = "INSERT INTO `weibo_checkin_poi` " + \
              "(`poiid`, `title`, `area_id`, `category_name`, `lon`, `lat`, " + \
              "`icon`, `poi_pic`, `task_id`, `checkin_user_num`, `checkin_num`, `lat_baidu`, `lon_baidu`)" + \
              "VALUES(?,?,?,?,?,?,?,?,?,?,?,null,null)"
        args = (poi["poiid"], poi["title"], areaid, poi["category_name"], float(poi["lon"]), float(poi["lat"]),
                poi["icon"], poi["poi_pic"], taskid, int(poi["checkin_user_num"]), int(poi["checkin_num"]))
        res = self.mysql_execute(sql, args, log=False)
        if res == 1:
            self.redis_conn.hincrby("poi_task_" + str(taskid) + "_worker_" + str(self.worker_id), "poi_add_count")
            return True
        return False

    def save_checkin(self, checkin):
        sql = "SELECT * from `weibo_checkin_checkin` where `mid` = ?"
        logging.info(checkin)
        res = self.mysql_select(sql, (checkin["mid"],))
        if len(res) != 0:
            return False
        time_obj = time.strptime(checkin["created_at"], "%a %b %d %H:%M:%S %z %Y")
        time_str = time.strftime("%Y-%m-%d %H:%M:%S", time_obj)

        sql = "INSERT INTO `weibo_checkin_checkin` " + \
              "(`mid`, `text`, `created_at`, `user_name`, `poi_id`) " + \
              "VALUES(?,?,?,?,?)"
        try:
            args = (checkin["mid"], checkin["text"][0:20], time_str,
                    checkin["user"]["name"], checkin["annotations"][0]["place"]["poiid"])
        except KeyError:
            # 比如，有些deleted为1的条目没有checkin["user']
            return False

        try:
            res = self.mysql_execute(sql, args)
        except OperationalError:
            logging.warning("incorrect charater found in: %s" % checkin["text"])
            args = (checkin["mid"], "[该微博包括特殊字符，不能存储。]", time_str,
                    checkin["user"]["name"], checkin["annotations"][0]["place"]["poiid"])
            res = self.mysql_execute(sql, args)
        if res == 1:
            return True
        return False

    def get_checkins_at(self, poiid):
        if self.weibo_client.is_expires():
            self.get_weibo_token(config["weibo_apps"][0]["app_key"],
                                 config["weibo_apps"][0]["app_secret"],
                                 config["weibo_apps"][0]["callback_url"],
                                 config["weibo_apps"][0]["accounts"][0]["username"],
                                 config["weibo_apps"][0]["accounts"][0]["password"],
                                 )
        page = 1
        if self.redis_conn.exists("checkin_task_" + poiid + "_page"):
            page = int(self.redis_conn.get("checkin_task_" + poiid + "_page"))
        res = self.weibo_client.place.poi_timeline.get(poiid=poiid, page=page, count=50)
        while True:
            if not res:
                break
            if not res["statuses"]:
                break
            page_add = 0
            last_time = None
            if isinstance(res["statuses"], list):
                for item in res["statuses"]:
                    last_time = time.strptime(item["created_at"], "%a %b %d %H:%M:%S %z %Y")
                    if self.save_checkin(item):
                        page_add += 1
            else:
                for key in res["statuses"].keys():
                    last_time = time.strptime(res["statuses"][key]["created_at"], "%a %b %d %H:%M:%S %z %Y")
                    if self.save_checkin(res["statuses"][key]):
                        page_add += 1
            logging.info("checkin page: %s, total: %s, add: %s" %
                         (page, len(res["statuses"]), page_add))

            # 只取最近一个月的。
            if time.mktime(last_time) + 3600 * 24 * 30 < time.mktime(time.localtime()):
                break

            page += 1
            self.redis_conn.set("checkin_task_" + poiid + "_page", page)

            if page > 100:
                break

            if self.weibo_client.is_expires():
                self.get_weibo_token(config["weibo_apps"][0]["app_key"],
                                     config["weibo_apps"][0]["app_secret"],
                                     config["weibo_apps"][0]["callback_url"],
                                     config["weibo_apps"][0]["accounts"][0]["username"],
                                     config["weibo_apps"][0]["accounts"][0]["password"],
                                     )
            time.sleep(2)
            res = self.weibo_client.place.poi_timeline.get(poiid=poiid, page=page, count=50)

        self.redis_conn.delete("checkin_task_" + poiid + "_page")

    def get_pois_at(self, lon, lat, taskid):
        # time.sleep(2)
        # return
        if self.weibo_client.is_expires():
            self.get_weibo_token(config["weibo_apps"][0]["app_key"],
                                 config["weibo_apps"][0]["app_secret"],
                                 config["weibo_apps"][0]["callback_url"],
                                 config["weibo_apps"][0]["accounts"][0]["username"],
                                 config["weibo_apps"][0]["accounts"][0]["password"],
                                 )
        page = 1
        if self.redis_conn.exists("poi_task_" + taskid + "_page"):
            page = int(self.redis_conn.get("poi_task_" + taskid + "_page"))

        res = self.weibo_client.place.nearby.pois.get(lat=lat, long=lon, page=page, range=100, count=50)
        while res:
            page_add = 0
            cur_poiid = ""
            if self.redis_conn.exists("poi_task_" + taskid + "_poiid"):
                cur_poiid = self.redis_conn.get("poi_task_" + taskid + "_poiid")

            for item in res["pois"]:
                if self.save_poi(item, taskid):
                    page_add += 1
                    self.get_checkins_at(item["poiid"])
                    self.redis_conn.set("poi_task_" + taskid + "_poiid", item["poiid"])
                else:
                    # 获取签到信息。
                    if cur_poiid == "":
                        continue
                    else:
                        if cur_poiid == item["poiid"]:
                            self.get_checkins_at(item["poiid"])
                            cur_poiid = ""

            self.redis_conn.delete("poi_task_" + taskid + "_poiid")

            logging.info("lon: %s, lat: %s, page: %s, total: %s, add: %s" %
                         (lon, lat, page, len(res["pois"]), page_add))

            page += 1
            self.redis_conn.set("poi_task_" + taskid + "_page", page)

            if self.weibo_client.is_expires():
                self.get_weibo_token(config["weibo_apps"][0]["app_key"],
                                     config["weibo_apps"][0]["app_secret"],
                                     config["weibo_apps"][0]["callback_url"],
                                     config["weibo_apps"][0]["accounts"][0]["username"],
                                     config["weibo_apps"][0]["accounts"][0]["password"],
                                     )
            time.sleep(2)
            res = self.weibo_client.place.nearby.pois.get(lat=lat, long=lon, page=page, range=100, count=50)
        self.redis_conn.delete("poi_task_" + taskid + "_page")

    def execute_poi_task(self, taskid):
        """ 开始/继续一个任务 """

        try:
            pid = os.fork()
            if pid == 0:  # 在子进程中进行任务。
                # 得到当前任务进行的信息。
                # 只要没有检测到暂停指令，就继续进行任务。
                logging.info("[%s]execute poi task #%s." % (os.getpid(), taskid))

                task = self.get_poi_task_x_worker_self(taskid)

                lon_total = int((task.max_lon - task.min_lon) / self.delta_latlon) + 1
                lat_total = int((task.max_lat - task.min_lat) / self.delta_latlon) + 1
                logging.info("lon_total: %s, lat_total: %s" % (lon_total, lat_total))

                while not self.redis_conn.exists("poi_" + str(taskid) + "_to_pause"):
                    # 任务完成
                    if task.cur_lat > task.max_lat:
                        self.redis_conn.hset("poi_task_" + str(taskid) + "_worker_" + str(self.worker_id),
                                             "progress", 100)
                        self.redis_conn.lpop("poi_worker_" + str(self.worker_id) + "_doing_list")
                        logging.info("poi task #%s finish." % taskid)
                        sys.exit()
                    try:
                        # 下载数据
                        self.get_pois_at(task.cur_lon, task.cur_lat, taskid)

                        # 计算progress
                        lon = round((task.cur_lon - task.min_lon) / self.delta_latlon) + 1
                        lat = round((task.cur_lat - task.min_lat) / self.delta_latlon) + 1
                        progress = round((lon_total * (lat - 1) + lon) / (lat_total * lon_total) * 100)
                        # if progress > 20:
                        #     raise WeiboLoginError(0, "test error. (progress > 20)")
                        self.redis_conn.hset("poi_task_" + str(taskid) + "_worker_" + str(self.worker_id),
                                             "progress", progress)
                        logging.info("cur_lon: %s, cur_lat: %s, cur_progress: %s" %
                                     (task.cur_lon, task.cur_lat, progress))
                        # 设置新的cur_lat、cur_lon
                        new_lat = task.cur_lat
                        new_lon = task.cur_lon + self.delta_latlon
                        if new_lon > task.max_lon:
                            new_lon = task.min_lon
                            new_lat = task.cur_lat + self.delta_latlon
                        self.redis_conn.hset("poi_task_" + str(taskid) + "_worker_" + str(self.worker_id),
                                             "cur_lon", new_lon)
                        self.redis_conn.hset("poi_task_" + str(taskid) + "_worker_" + str(self.worker_id),
                                             "cur_lat", new_lat)

                        task = self.get_poi_task_x_worker_self(taskid)
                    except WeiboLoginError as e:
                        errmsg = str(e)
                        self.redis_conn.lpop("poi_worker_" + str(self.worker_id) + "_doing_list")
                        logging.info("poi task #%s error: %s" % (taskid, errmsg))
                        self.redis_conn.hset("poi_task_" + str(taskid) + "_worker_" + str(self.worker_id),
                                             "errormsg", errmsg)
                        sys.exit()
                    except BaseException:
                        # 任务出错
                        errmsg = "some reason i don't know"
                        self.redis_conn.lpop("poi_worker_" + str(self.worker_id) + "_doing_list")
                        logging.info("poi task #%s error: %s" % (taskid, errmsg))
                        self.redis_conn.hset("poi_task_" + str(taskid) + "_worker_" + str(self.worker_id),
                                             "errormsg", errmsg)
                        raise
                    time.sleep(2)  # 每次网络请求的间隔
                # 由于用户主动暂停而终止：
                self.redis_conn.lpop("poi_worker_" + str(self.worker_id) + "_doing_list")
                self.redis_conn.hset("poi_task_" + str(taskid) + "_worker_" + str(self.worker_id),
                                     "errormsg", "用户暂停")
                logging.info("task #%s paused." % taskid)

                # todo: 更新cur_lat和cur_lon
                # todo: 这里的doing_list只能容纳一个元素
                sys.exit()
        except OSError as err:
            sys.stderr.write('fork failed: {0}\n'.format(err))
            sys.exit(1)


if __name__ == '__main__':
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(filename)s[line:%(lineno)d]\n\t%(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filename='log/worker_daemon_%s.log' % (time.strftime("%Y-%m-%d", time.localtime()))
    )

    # 守护进程
    daemon = WorkerDaemon('/tmp/worker_daemon.pid',
                          workerid=config["worker_id"],
                          stdout="/dev/stdout",
                          stderr="/dev/stdout")
    daemon.read_weibo_apps(config["weibo_apps"])

    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print('unknown command')
            sys.exit(2)
        sys.exit(0)
    else:
        print('usage: %s start|stop|restart' % sys.argv[0])
        sys.exit(2)
