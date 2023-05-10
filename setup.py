from setuptools import setup

setup(name='oltools',
      version='0.1',
      description='Tools for Open Library dumps',
      url='http://github.com/silviot/oltools',
      author='Flying Circus',
      author_email='silvio@tomatis.email',
      license='MIT',
      packages=['oltools'],
      install_requires=[],
      extras_require={
          "test": ["pytest"],
      },
      zip_safe=False)
