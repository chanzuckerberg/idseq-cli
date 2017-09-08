from setuptools import setup

setup(name='idseq',
      version='0.1',
      description='Idseq CLI',
      url='http://github.com/chanzuckerberg/idsdeq-cli',
      author='Chan Zuckerberg Initiative, LLC',
      author_email='rking@chanzuckerberg.com',
      license='MIT',
      packages=['idseq'],
      zip_safe=False,
      install_requires=['requests', 'tqdm'],
      entry_points={'console_scripts': ['idseq=idseq:main']},
      extras_require={'dev': ['flake8']})
