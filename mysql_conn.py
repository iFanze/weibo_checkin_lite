import logging
import MySQLdb
import sys


class MySQLConn:
    def __init__(self, host="localhost", port=3306, db="", user="", passwd=""):
        self.mysql_conn = MySQLdb.connect(host=host, port=port, db=db, user=user, passwd=passwd)

    def mysql_select(self, sql, args, size=None, log=True):
        if log:
            logging.info(sql + '\n\t' + str(args))
        cur = self.mysql_conn.cursor(MySQLdb.cursors.DictCursor)
        try:
            cur.execute(sql.replace('?', '%s'), args or ())
            if size:
                if size == 1:
                    rs = cur.fetchone()
                else:
                    rs = cur.fetchmany(size)
            else:
                rs = cur.fetchall()
        except BaseException:
            print("Unexpected error:", sys.exc_info()[0])
            raise
        finally:
            cur.close()
        if log:
            if rs:
                rss = len(rs)
            else:
                rss = 0
            logging.info('rows returned: %s' % rss)
        return rs

    def mysql_execute(self, sql, args, autocommit=True, log=True):
        if log:
            logging.info(sql + '\n\t' + str(args))
        cur = self.mysql_conn.cursor(MySQLdb.cursors.DictCursor)
        try:
            cur.execute(sql.replace('?', '%s'), args or ())
            affected = cur.rowcount
            if autocommit:
                self.mysql_conn.commit()
        except BaseException as e:
            print("Unexpected error:", sys.exc_info()[0])
            raise
        finally:
            cur.close()
        if log:
            logging.info('rows affected: %s' % affected)
        return affected
