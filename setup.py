from setuptools import setup

setup(name="sqlitenumpy",
      version="1.0.0",
      description="Helper methods for converting to and from SQLite to numpy.",
      py_modules=['sqlitenumpy'],
      license='MIT',
      url='https://github.com/jonwoodring/sqlitenumpy',
      install_requires=['numpy'],
      test_suite='tests'
      )
