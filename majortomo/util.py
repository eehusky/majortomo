"""Utilities and helpers useful in other modules
"""

from typing import Text


TextOrBytes = Text|bytes

def ensure_binary(s:str|bytes, encoding:str='utf-8', errors:str='strict'):
    """Coerce **s** to six.binary_type.

    For Python 2:
      - `unicode` -> encoded to `str`
      - `str` -> `str`

    For Python 3:
      - `str` -> encoded to `bytes`
      - `bytes` -> `bytes`
    """
    if isinstance(s, bytes):
        return s
    if isinstance(s, str):
        return s.encode(encoding, errors)
    raise TypeError("not expecting type '%s'" % type(s))

def text_to_ascii_bytes(text:TextOrBytes)->bytes:
    """Convert a text-or-bytes value to ASCII-encoded bytes

    If the input is already `bytes`, we simply return it as is
    """
    return ensure_binary(text, 'ascii')
