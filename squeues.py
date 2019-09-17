"""
Scheduler queues
"""

import marshal
from six.moves import cPickle as pickle

from queuelib import queue


def _serializable_queue(queue_class, serialize, deserialize):
    """基于磁盘的任务队列类，每次执行后会把队列任务保存到磁盘上"""

    class SerializableQueue(queue_class):

        def push(self, obj):
            s = serialize(obj)
            super(SerializableQueue, self).push(s)

        def pop(self):
            s = super(SerializableQueue, self).pop()
            if s:
                return deserialize(s)

    return SerializableQueue


def _pickle_serialize(obj):
    try:
        return pickle.dumps(obj, protocol=2)
    # Python <= 3.4 raises pickle.PicklingError here while
    # 3.5 <= Python < 3.6 raises AttributeError and
    # Python >= 3.6 raises TypeError
    except (pickle.PicklingError, AttributeError, TypeError) as e:
        raise ValueError(str(e))


PickleFifoDiskQueue = _serializable_queue(queue.FifoDiskQueue,
    _pickle_serialize, pickle.loads)  # 基于磁盘，先进先出
PickleLifoDiskQueue = _serializable_queue(queue.LifoDiskQueue,
    _pickle_serialize, pickle.loads)  # 基于磁盘，后进先出
MarshalFifoDiskQueue = _serializable_queue(queue.FifoDiskQueue,
    marshal.dumps, marshal.loads)
MarshalLifoDiskQueue = _serializable_queue(queue.LifoDiskQueue,
    marshal.dumps, marshal.loads)
FifoMemoryQueue = queue.FifoMemoryQueue  # 基于内存，先进先出
LifoMemoryQueue = queue.LifoMemoryQueue  # 基于内存，后进先出，每次都在内存中执行，下次启动则消失；
