import random
import uuid

from sqlalchemy import BIGINT, BINARY
from uuid_fastertime import uuid1_fastertime

from uuid6 import uuid6, uuid7


class KeyGenerator:

    def get_next(self):
        raise NotImplementedError()

    def datatype(self):
        raise NotImplementedError()


class RandomInt64KeyGenerator(KeyGenerator):
    def get_next(self):
        return random.getrandbits(63)  # -1 bit because we're using singed numbers

    def datatype(self):
        return BIGINT


class SequentialInt64KeyGenerator(KeyGenerator):
    def __init__(self):
        self._cur_val = 0

    def get_next(self):
        self._cur_val +=1
        return self._cur_val

    def datatype(self):
        return BIGINT


class UUID1KeyGenerator(KeyGenerator):
    def get_next(self):
        return uuid.uuid1().bytes

    def datatype(self):
        return BINARY(length=16)


class UUID1FastRolloverKeyGenerator(KeyGenerator):
    def get_next(self):
        return uuid1_fastertime(time_multiplier=1000).bytes

    def datatype(self):
        return BINARY(length=16)


class UUID4KeyGenerator(KeyGenerator):
    def get_next(self):
        return uuid.uuid4().bytes

    def datatype(self):
        return BINARY(length=16)


class UUID6KeyGenerator(KeyGenerator):
    def get_next(self):
        return uuid6().bytes

    def datatype(self):
        return BINARY(length=16)


class UUID7KeyGenerator(KeyGenerator):
    def get_next(self):
        return uuid7().bytes

    def datatype(self):
        return BINARY(length=16)


