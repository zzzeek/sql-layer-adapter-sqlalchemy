import os
import re

from setuptools import setup, find_packages

v = open(os.path.join(os.path.dirname(__file__), 'sqlalchemy_foundationdb', '__init__.py'))
VERSION = re.compile(r".*__version__ = '(.*?)'", re.S).match(v.read()).group(1)
v.close()

readme = os.path.join(os.path.dirname(__file__), 'README.rst')


setup(name='sqlalchemy-foundationdb',
      version=VERSION,
      description="FoundationDB SQL Layer Dialect and ORM Extension for SQLAlchemy",
      long_description=open(readme).read(),
      classifiers=[
      'Development Status :: 3 - Alpha',
      'Environment :: Console',
      'Intended Audience :: Developers',
      'Programming Language :: Python',
      'Programming Language :: Python :: 3',
      'Programming Language :: Python :: Implementation :: CPython',
      'Programming Language :: Python :: Implementation :: PyPy',
      'Topic :: Database :: Front-Ends',
      ],
      keywords='FoundationDB SQLAlchemy',
      author='Mike Bayer',
      author_email='mike@zzzcomputing.com',
      license='MIT',
      packages=find_packages('.', exclude=['examples*', 'test*']), 
      install_requires=['fdb_sql >= 0.9dev', 'sqlalchemy >= 0.9.2'],
      include_package_data=True,
      tests_require=['nose >= 0.11'],
      test_suite="run_tests",
      zip_safe=False,
      entry_points={
         'sqlalchemy.dialects': [
              'foundationdb = sqlalchemy_foundationdb.dialect.psycopg2:FDBPsycopg2Dialect',
              'foundationdb.psycopg2 = sqlalchemy_foundationdb.dialect.psycopg2:FDBPsycopg2Dialect',
              ]
        }
)
