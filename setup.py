from setuptools import setup, find_packages
from pkg_resources import resource_string

VERSION_STR = __import__('sqlalchemy_foundationdb').__version__
README_STR = resource_string(__name__, 'README.rst')

setup(name='sqlalchemy-foundationdb',
      version=VERSION_STR,
      description="FoundationDB SQL Layer Dialect and ORM Extension for SQLAlchemy",
      long_description=README_STR,
      url="https://github.com/FoundationDB/sql-layer-adapter-sqlalchemy",
      classifiers=[
      'Development Status :: 4 - Beta',
      'Environment :: Console',
      'Intended Audience :: Developers',
      'License :: OSI Approved :: MIT License',
      'Programming Language :: Python',
      'Programming Language :: Python :: 3',
      'Programming Language :: Python :: Implementation :: CPython',
      'Programming Language :: Python :: Implementation :: PyPy',
      'Topic :: Database :: Front-Ends',
      ],
      keywords='FoundationDB SQLAlchemy',
      author='Mike Bayer',
      author_email='mike@zzzcomputing.com',
      maintainer= 'FoundationDB',
      maintainer_email='distribution@foundationdb.com',
      license='MIT',
      packages=find_packages(exclude=['test']),
      install_requires=['foundationdb_sql >= 0.9dev', 'sqlalchemy >= 0.9.2'],
      include_package_data=True,
      tests_require=['pytest >= 2.5.2', 'mock >= 1.0.1'],
      test_suite="pytest.main",
      zip_safe=True,
      entry_points={
         'sqlalchemy.dialects': [
              'foundationdb = sqlalchemy_foundationdb.dialect.psycopg2:FDBPsycopg2Dialect',
              'foundationdb.psycopg2 = sqlalchemy_foundationdb.dialect.psycopg2:FDBPsycopg2Dialect',
              ]
        }
)
