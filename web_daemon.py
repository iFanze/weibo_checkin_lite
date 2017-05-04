import logging
import sys
import os
import time

from daemon import Daemon
from mysql_conn import MySQLConn
from redis_conn import RedisConn


class WebDaemon(Daemon, MySQLConn, RedisConn):
    def __init__(self, pidfile, stdin=os.devnull, stdout=os.devnull, stderr=os.devnull):
        # 初始化Daemon
        super(WebDaemon, self).__init__(pidfile, stdin, stdout, stderr)
        # 初始化MySQLConn
        super(Daemon, self).__init__(db="weibo_checkin", user="root", passwd="admin")
        # 初始化Redis
        super(MySQLConn, self).__init__()
        logging.info("web daemon init.")

    def run(self):
        while True:
            sys.stdout.write('.')
            sys.stdout.flush()

            self.mysql_conn.ping(True)

            # 对于要继续进行的任务：
            task_todo = self.redis_conn.lpop("poi_task_todo_list")
            if task_todo:
                logging.info("poi_todo found, taskid: %s" % task_todo)
                self.redis_conn.rpush("poi_task_doing_list", task_todo)
                self.redis_conn.rpush("poi_worker_1_todo_list", task_todo)
                self.redis_conn.hset("poi_task_" + task_todo + "_worker_1", "errormsg", "")

            # 监视所有poi_worker_*_doing_list：
            tasks_doing = self.redis_conn.lrange("poi_task_doing_list", 0, -1)
            for task_doing in tasks_doing:
                workers_count = self.redis_conn.llen("poi_task_" + task_doing + "_worker_list")
                logging.info("task #%s is working by %s workers." % (task_doing, workers_count))
                workers = self.redis_conn.lrange("poi_task_" + task_doing + "_worker_list", 0, -1)
                # 任务0：如果任务需要暂停
                while self.redis_conn.exists("poi_" + str(task_doing) + "_to_pause"):
                    logging.info("task #%s should be paused, waitting for workers." % task_doing)
                    is_paused = 0
                    for worker in workers:
                        if self.redis_lfind("poi_worker_" + str(worker) + "_doing_list", task_doing) < 0:
                            is_paused += 1
                    if is_paused == workers_count:
                        # 所有worker都已暂停
                        self.redis_conn.lrem("poi_task_doing_list", task_doing)
                        self.mysql_execute("update `weibo_checkin_poitask` set `status`= 3, `last_error` = '用户暂停' " +
                                           "where id = ?",
                                           (task_doing,))
                        self.redis_conn.delete("poi_" + str(task_doing) + "_to_pause")
                        break
                    else:
                        time.sleep(2)
                        continue
                    logging.info("task #%s paused successfully." % task_doing)

                # 任务1：更新progress
                avg_progress = 0
                total_add = 0
                for worker in workers:
                    progress = self.redis_conn.hget("poi_task_" + task_doing + "_worker_" + worker, "progress")
                    avg_progress += int(progress)
                    total_add += int(self.redis_conn.hget("poi_task_" + task_doing + "_worker_" + worker,
                                                          "poi_add_count"))
                avg_progress /= workers_count
                avg_progress = int(avg_progress)
                logging.info("task #%s progress: %s%%, poi add: %s" % (task_doing, avg_progress, total_add))

                self.mysql_execute("update `weibo_checkin_poitask` set `progress`= ?, `poi_add_count` = ? where id = ?",
                                   (avg_progress, total_add, task_doing))

                # 任务2：监控worker
                is_done = 0
                error_found = False
                error_list = []
                for worker in workers:
                    # 有worker还没开始
                    if 0 <= self.redis_lfind("poi_worker_" + worker + "_todo_list", task_doing):
                        continue
                    # 有worker还没结束
                    if 0 <= self.redis_lfind("poi_worker_" + worker + "_doing_list", task_doing):
                        continue
                    errormsg = self.redis_conn.hget("poi_task_" + task_doing + "_worker_" + worker, "errormsg")
                    if errormsg:
                        error_found = True
                        error_list.append("Worker #%s error: %s" % (worker, errormsg))
                    is_done += 1

                # 只有部分worker停止工作。
                if is_done != workers_count:
                    logging.info("%s of %s workers is done." % (is_done, workers_count))
                    continue

                # 运行到这里已经没有worker在工作了。
                if error_found:
                    # 所有worker完成，但是出现了错误。
                    logging.info("all workers done with error found: %s" % error_list)
                    self.mysql_execute("update `weibo_checkin_poitask` set `status` = 3, `last_error` = ? where id = ?",
                                       (';'.join(error_list), task_doing))
                else:
                    # 所有worker顺利完成。更新task的状态和area的POI总数。
                    logging.info("all workers done without error.")
                    self.mysql_execute("update `weibo_checkin_poitask` set `status` = 4, `last_error` = '' " +
                                       "where id = ?", (task_doing,))
                    sql = "SELECT `area_id` from `weibo_checkin_poitask` where `id` = ?"
                    res = self.mysql_select(sql, (task_doing,), 1, log=False)
                    areaid = int(res["area_id"])
                    sql = "SELECT count(*) count from `weibo_checkin_poitask` where `area_id` = ?"
                    res = self.mysql_select(sql, (areaid,), 1)
                    count = res["count"]
                    self.mysql_execute("update `weibo_checkin_area` set `poi_count` = ? " +
                                       "where id = ?", (count, areaid))

                # 从doing_list清除，不再监控。
                self.redis_conn.lrem("poi_task_doing_list", task_doing)
            time.sleep(2)


if __name__ == '__main__':
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(filename)s[line:%(lineno)d]\n\t%(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filename='log/web_daemon_%s.log' % (time.strftime("%Y-%m-%d", time.localtime()),)
    )

    # 守护进程
    daemon = WebDaemon('/tmp/web_daemon.pid', stdout="/dev/stdout")
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
