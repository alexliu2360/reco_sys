import os
import sys

import redis

from setting.default import RAParam

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))

from server import redis_client
from server import pool
import logging
from datetime import datetime
from server.utils import HBaseUtils

logger = logging.getLogger('recommend')


class ReadRecall(object):
    """读取召回集的结果
    """
    def __init__(self):
        self.client = redis_client
        self.hbu = HBaseUtils(pool)

    def read_hbase_recall_data(self, table_name, key_format, column_format):
        """获取指定用户的对应频道的召回结果,在线画像召回，离线画像召回，离线协同召回
        :return:
        """
        # 获取family对应的值
        # 数据库中的键都是bytes类型，所以需要进行编码相加
        # 读取召回结果多个版本合并
        recall_list = []
        try:

            data = self.hbu.get_table_cells(table_name, key_format, column_format)
            for _ in data:
                recall_list = list(set(recall_list).union(set(eval(_))))

            # 读取所有这个用户的在线推荐的版本，清空该频道的数据
            # self.hbu.get_table_delete(table_name, key_format, column_format)
        except Exception as e:
            logger.warning(
                "{} WARN read recall data exception:{}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e))
        return recall_list

    def read_redis_new_data(self, channel_id):
        """获取redis新文章结果
        :param channel_id:
        :return:
        """
        # format结果
        logger.info("{} INFO read channel:{} new recommend data".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), channel_id))
        _key = "ch:{}:new".format(channel_id)
        try:
            res = self.client.zrevrange(_key, 0, -1)
        except redis.exceptions.ResponseError as e:
            logger.warning("{} WARN read new article exception:{}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e))
            res = []
        return list(map(int, res))

    def read_redis_hot_data(self, channel_id):
        """获取redis热门文章结果
        :param channel_id:
        :return:
        """
        # format结果
        logger.info("{} INFO read channel:{} hot recommend data".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), channel_id))
        _key = "ch:{}:hot".format(channel_id)
        try:
            _res = self.client.zrevrange(_key, 0, -1)
        except redis.exceptions.ResponseError as e:
            logger.warning("{} WARN read hot article exception:{}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e))
            _res = []
        # 每次返回前50热门文章
        res = list(map(int, _res))
        if len(res) > 50:
            res = res[:50]
        return res

    def read_hbase_article_similar(self, table_name, key_format, article_num):
        """获取文章相似结果
        :param article_id: 文章id
        :param article_num: 文章数量
        :return:
        """
        # 第一种表结构方式测试：
        # create 'article_similar', 'similar'
        # put 'article_similar', '1', 'similar:1', 0.2
        # put 'article_similar', '1', 'similar:2', 0.34
        try:
            _dic = self.hbu.get_table_row(table_name, key_format)

            res = []
            _srt = sorted(_dic.items(), key=lambda obj: obj[1], reverse=True)
            if len(_srt) > article_num:
                _srt = _srt[:article_num]
            for _ in _srt:
                res.append(int(_[0].decode().split(':')[1]))
        except Exception as e:
            logger.error("{} ERROR read similar article exception: {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e))
            res = []
        return res


if __name__ == '__main__':

    rr = ReadRecall()
    print(rr.read_hbase_article_similar('article_similar', b'13342', 10))
    print(rr.read_hbase_recall_data('cb_recall', b'recall:user:1115629498121846784', b'als:18'))

    # rr = ReadRecall()
    # print(rr.read_redis_new_data(18))