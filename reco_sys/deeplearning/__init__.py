# coding='utf8'
import keras
import tensorflow as tf
vocab_size = 5000
sentence_size = 200
imdb = keras.datasets.imdb

(x_train_variable, y_train), (x_test_variable, y_test) = imdb.load_data(num_words=vocab_size)
x_train = keras.preprocessing.sequence.pad_sequences(
    x_train_variable,
    maxlen=sentence_size,
    padding='post',
    value=0)
x_test = keras.preprocessing.sequence.pad_sequences(
    x_test_variable,
    maxlen=sentence_size,
    padding='post',
    value=0)

# keras.preprocessing.sequence.pad_sequences(sequences, maxlen=None, dtype='int32',
#     padding='pre', truncating='pre', value=0.)

def parser(x, y):
    features = {"x": x}
    return features, y

def train_input_fn():
    dataset = tf.data.Dataset.from_tensor_slices((x_train, y_train))
    dataset = dataset.shuffle(buffer_size=len(x_train_variable))
    dataset = dataset.batch(100)
    dataset = dataset.map(parser)
    dataset = dataset.repeat()
    iterator = dataset.make_one_shot_iterator()
    return iterator.get_next()


def eval_input_fn():
    dataset = tf.data.Dataset.from_tensor_slices((x_test, y_test))
    dataset = dataset.batch(100)
    dataset = dataset.map(parser)
    iterator = dataset.make_one_shot_iterator()
    return iterator.get_next()

column = tf.feature_column.categorical_column_with_identity('feature', vocab_size)

embedding_size = 50
word_embedding_column = tf.feature_column.embedding_column(
    column, dimension=embedding_size
)
classifier = tf.estimator.DNNClassifier(
    hidden_units=[100],
    feature_columns=[word_embedding_column],
    model_dir='./tmp/embeddings'
)

classifier.train(input_fn=train_input_fn, steps=25000)
eval_results = classifier.evaluate(input_fn=eval_input_fn)
print(eval_results)
