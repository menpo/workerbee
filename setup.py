from os.path import join
from setuptools import setup, find_packages
import sys
import versioneer

requirements = [
      'postgres>=2.2.1',
      'psycopg2>=2.6.1',
      'docopt>=0.6',
      'arrow>=0.7.0',
]

if sys.version_info.major == 2:
    requirements.extend(['pathlib>=1.0'])

setup(name='workerbee',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='Simple functional framework for embarrassingly parallel jobs',
      author='The Menpo Team',
      author_email='hello@menpo.org',
      packages=find_packages(),
      scripts=[join('bin', 'workerbee')],
      install_requires=requirements
)
