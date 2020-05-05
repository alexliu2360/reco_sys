# coding='utf8'
import tensorflow as tf

FEATURE_COLUMNS = ['channel_id', 'vector', 'user_weigths', 'article_weights']


class LrWithFtrl(object):
    """LR以FTRL方式优化
    """
    def __init__(self):
        pass

    @staticmethod
    def read_ctr_records():
        # 定义转换函数,输入时序列化的
        def parse_tfrecords_function(example_proto):
            features = {
                "label": tf.FixedLenFeature([], tf.int64),
                "feature": tf.FixedLenFeature([], tf.string)
            }
            parsed_features = tf.parse_single_example(example_proto, features)

            feature = tf.decode_raw(parsed_features['feature'], tf.float64)
            feature = tf.reshape(tf.cast(feature, tf.float32), [1, 121])
            # 特征顺序 1 channel_id,  100 article_vector, 10 user_weights, 10 article_weights
            # 1 channel_id类别型特征， 100维文章向量求平均值当连续特征，10维用户权重求平均值当连续特征
            channel_id = tf.cast(tf.slice(feature, [0, 0], [1, 1]), tf.int32)
            vector = tf.reduce_sum(tf.slice(feature, [0, 1], [1, 100]), axis=1)
            user_weights = tf.reduce_sum(tf.slice(feature, [0, 101], [1, 10]), axis=1)
            article_weights = tf.reduce_sum(tf.slice(feature, [0, 111], [1, 10]), axis=1)

            label = tf.cast(parsed_features['label'], tf.float32)

            # 构造字典 名称-tensor
            tensor_list = [channel_id, vector, user_weights, article_weights]

            feature_dict = dict(zip(FEATURE_COLUMNS, tensor_list))

            return feature_dict, label

        dataset = tf.data.TFRecordDataset(["./train_ctr_20190605.tfrecords"])
        dataset = dataset.map(parse_tfrecords_function)
        dataset = dataset.batch(64)
        dataset = dataset.repeat(10)
        return dataset

    def train_eval(self):
        """
        训练模型
        :return:
        """
        # 离散分类
        article_id = tf.feature_column.categorical_column_with_identity('channel_id', num_buckets=25)
        # 连续类型
        vector = tf.feature_column.numeric_column('vector')
        user_weigths = tf.feature_column.numeric_column('user_weigths')
        article_weights = tf.feature_column.numeric_column('article_weights')

        feature_columns = [article_id, vector, user_weigths, article_weights]

        classifiry = tf.estimator.LinearClassifier(feature_columns=feature_columns,
                                                   optimizer=tf.train.FtrlOptimizer(learning_rate=0.1,
                                                                                    l1_regularization_strength=10,
                                                                                    l2_regularization_strength=10))
        classifiry.train(LrWithFtrl.read_ctr_records, steps=10000)
        result = classifiry.evaluate(LrWithFtrl.read_ctr_records)
        print(result)


if __name__ == '__main__':
    lwf = LrWithFtrl()
    lwf.train_eval()