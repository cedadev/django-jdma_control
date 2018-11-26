import os
from setuptools import setup

with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as readme:
    README = readme.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='jdma_control',
    version='0.2.9',
    packages=['jdma_control'],
    install_requires=[
        'appdirs',
        'beautifulsoup4',
        'django',
        'django-sizefield',
        'django-extensions',
        'django-multiselectfield',
        'html5lib',
        'psycopg2-binary',
        'packaging',
        'pyparsing',
        'pytz',
        'six',
        'feedgen',
        'feedparser',
        'jasmin-ldap',
        'pycryptodome',
        'boto3'
    ],
    include_package_data=True,
    license='my License',  # example license
    description=('A Django app to migrate directories of files to external'
                 'storage from groupworkspaces on JASMIN.'),
    long_description=README,
    url='http://www.ceda.ac.uk/',
    author='Neil Massey',
    author_email='neil.massey@stfc.ac.uk',
    classifiers=[
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',  # example license
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        # Replace these appropriately if you are stuck on Python 2.
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
    ],
)
