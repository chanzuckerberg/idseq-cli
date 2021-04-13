from setuptools import setup

setup(name='idseq',
      version='0.8.14',
      description='IDseq CLI',
      url='http://github.com/chanzuckerberg/idseq-cli',
      author='Chan Zuckerberg Initiative, LLC',
      author_email='help@idseq.net',
      license='MIT',
      packages=['idseq'],
      zip_safe=False,
      install_requires=['future', 'requests'],
      entry_points={'console_scripts': ['idseq=idseq.cli:main']},
      extras_require={'dev': ['flake8']})
