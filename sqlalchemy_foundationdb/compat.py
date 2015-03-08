from sqlalchemy import __version__ as sa_version
import re

sa_version = tuple(int(x) for x in re.findall(r'(\d+)', sa_version))
sqla_09 = sa_version >= (0, 9, 0)
sqla_10 = sa_version >= (1, 0, 0)
