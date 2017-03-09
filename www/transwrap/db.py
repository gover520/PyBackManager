#coding=utf-8

__author__ = 'L.Y'

'''
Database operation module.
'''

import time
import uuid
import functools
import threading
import logging


# Dict 字典
'''
支持访问方式 x.y == x['y']
以及支持初始化多字段 Dict(('a', 'b', 'c'), (1, 2, 3)) == .a=1 .b=2 .c=3
'''
class Dict(dict):
    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value


# Lasy Connection to DB
class _LasyConnection(object):
    def __init__(self):
        self.connection = None

    def cursor(self):
        if self.connection is None:
            connection = _engine.connect()
            logging.info('[DB] open connection <%s>...' % hex(id(connection)))
            self.connection = connection
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection:
            connection = self.connection
            self.connection = None
            logging.info('[DB] close connection <%s>...' % hex(id(connection)))
            connection.close()


# DB Ctx with thread-local
class _DBCtx(threading.local):
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return not self.connection is None

    def init(self):
        logging.info('[DB] open lazy connection...')
        self.connection = _LasyConnection()
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()

    def cursor(self):
        return self.connection.cursor()


'''
Global Params
'''
# thread-local db context
_db_ctx = _DBCtx()

# engine object
_engine = None

class DBError(Exception):
    pass


# Engine 数据库驱动器
class _Engine(object):
    def __init__(self, connect):
        self._connect = connect

    def connect(self):
        return self._connect()

# Create Engine 创建DB Engine
def create_engine(user, pwd, database, host='127.0.0.1', port=3306, **kw):
    import pymysql

    global _engine
    if _engine is not None:
        raise DBError('Engine is already initalized...')

    params = dict(user=user, password=pwd, database=database, host=host, port=port)
    # 待查 collation & buffered
    # defaults = dict(use_unicode=True, charset='utf8', collation='utf8_general_ci', autocommit=False)
    defaults = dict(use_unicode=True, charset='utf8', autocommit=False)
    for k, v in defaults.iteritems():
        params[k] = kw.pop(k, v)
    params.update(kw)
    # params['buffered'] = True
    _engine = _Engine(lambda: pymysql.Connect(**params))

    # test connection..
    logging.info('Init mysql engine <%s> ok.' % hex(id(_engine)))


# 连接上下文
class _ConnectionCtx():
    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()

# wrap connection 封装连接
'''
@with_connection
def foo(**args, **kw):
    f1()
    f2()
    f3()
'''
def with_connection(func):
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with _ConnectionCtx():
            return func(*args, **kw)
    return _wrapper

# DB sql语句的封装
@with_connection
def _update(sql, **args):
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('[DB] Sql: %s, Args: %s' % (sql, args))

    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        r = cursor.rowcount
        if _db_ctx.transactions == 0:
            logging.info("auto commit")
            _db_ctx.connection.commit()
        return r
    finally:
        if cursor:
            cursor.close()

# 增
def insert(table, **kw):
    cols, args = zip(*kw.iteritems())
    sql = "insert into '%s' (%s)" % (table, ','.join(["'%s'" % col for col in cols])), ','.join(['?' for i in range(len(cols))])
    return _update(sql, *args)

# 改
def update(sql, *args):
    return _update(sql, *args)

# 查 为什么不需要@with_connection 看来我还是没搞懂这个东西
def _select(sql, first, *args):
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('[DB] Sql: %s, Args: %s' % (sql, args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        if cursor.description:
            names = [x[0] for x in cursor.description]
        if first:
            values = cursor.fetchone()
            if not values:
                return None
            return Dict(names, values)
        return [Dict(names, x) for x in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()

@with_connection
def select(sql, *args):
    return _select(sql, False, *args)



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    create_engine('root', '123456', 'test')
    update('drop table if exists user')
    update('create table user (id int primary key, name text, email text, password text, last_modified real)')