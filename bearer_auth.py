import base64
import binascii


class BearerAuth(object):
    """
    Inspired by BasicAuth from aiohttp
    """

    def __init__(self, key: str, encoding='utf-8'):
        self._key = key
        self._encoding = encoding

    @property
    def key(self) -> str:
        return self._key

    @classmethod
    def decode(cls, auth_header: str, encoding='utf-8') -> 'BearerAuth':
        split = auth_header.strip().split(' ')
        if len(split) == 2:
            if split[0].strip().lower() != 'bearer':
                raise ValueError('Unknown authorization method {}'.format(split[0]))
            encoded_key = split[1]
        else:
            raise ValueError('Could not parse authorization header')

        try:
            return cls(base64.b64decode(encoded_key.encode('ascii')).decode('utf-8'), encoding=encoding)
        except binascii.Error:
            raise ValueError('Invalid base64 encoding')

    def encode(self) -> str:
        return 'Bearer {}'.format(base64.b64encode(self._key.encode(self._encoding))).decode(self._encoding)

    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            return False

        return self._key.encode(self._encoding) == other._key.encode(self._encoding)

    def __repr__(self) -> str:
        return "{}('{}', encoding='{}')".format(self.__class__.__name__, self._key, self._encoding)
