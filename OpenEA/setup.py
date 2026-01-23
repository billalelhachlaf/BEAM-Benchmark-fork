"""Setup.py for OpenEA."""

import os

import setuptools

MODULE = 'openea'
VERSION = '1.0'
PACKAGES = setuptools.find_packages(where='src')
META_PATH = os.path.join('src', MODULE, '__init__.py')
KEYWORDS = ['Knowledge Graph', 'Embeddings', 'Entity Alignment']
INSTALL_REQUIRES = [
    # 'tensorflow',
    'pandas>=3.0.0',
    'matching>=1.4.0',
    'scikit-learn>=1.7.0',
    'numpy>=2.0.0',
    'gensim>=4.3.0',
    'python-Levenshtein>=0.27.0',
    'scipy>=1.14.0',
]

if __name__ == '__main__':
    setuptools.setup(
        name=MODULE,
        version=VERSION,
        description='A package for embedding-based entity alignment',
        url='https://github.com/nju-websoft/OpenEA.git',
        author='Zequn Sun',
        author_email='zqsun.nju@gmail.com',
        maintainer='Zequn Sun',
        maintainer_email='zqsun.nju@gmail.com',
        license='MIT',
        keywords=KEYWORDS,
        packages=setuptools.find_packages(where='src'),
        package_dir={'': 'src'},
        include_package_data=True,
        install_requires=INSTALL_REQUIRES,
        zip_safe=False,
    )
