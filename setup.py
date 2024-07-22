import os
from setuptools import setup

with open(os.path.join(os.path.dirname(__file__), 'README.md')) as readme:
    README = readme.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='jdma_control',
    version='1.0.0',
    packages=['jdma_control'],
    install_requires=[
        'appdirs',
        'beautifulsoup4',
        'boto3',
        'django==4.2',
        'django-extensions',
        'django-multiselectfield',
        'django-sizefield',
        'html5lib',
        'lxml',
        'jasmin-ldap @ git+https://github.com/cedadev/jasmin-ldap.git@v1.0.2#egg=jasmin_ldap',
        'elastic-tape @ svn+https://svn.esc.rl.ac.uk/repos/jasmin_et/code/tags/1.0.0.1/elastic_tape#egg=elastic_tape',
        'packaging',
        'psycopg2-binary',
        'pycryptodome',
        'pyparsing',
        'pytz',
        'requests'
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
