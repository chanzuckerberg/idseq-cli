from setuptools import setup

setup(name='idseq',
      version='0.8.1',
      description='IDseq CLI',
      url='http://github.com/chanzuckerberg/idsdeq-cli',
      author='Chan Zuckerberg Initiative, LLC',
      author_email='rking@chanzuckerberg.com',
      license='MIT',
      packages=['idseq'],
      zip_safe=False,
      install_requires=['future', 'requests'],
      entry_points={'console_scripts': ['idseq=idseq.cli:main']},
      extras_require={'dev': ['flake8']})
