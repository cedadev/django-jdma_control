
# -*- coding: utf-8 -*-

import os


# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


DEBUG = True
# Security settings
SECURE_CONTENT_TYPE_NOSNIFF = True
SECURE_BROWSER_XSS_FILTER = True
SESSION_COOKIE_SECURE = True
X_FRAME_OPTIONS = 'DENY'
ALLOWED_HOSTS = ['192.168.51.26']


# Read the secret key from a file
SECRET_KEY_FILE = '/home/vagrant/JDMA/conf/secret_key.txt'
with open(SECRET_KEY_FILE) as f:
    SECRET_KEY = f.read().strip()


# Logging settings
LOG_FORMAT = '[%(levelname)s] [%(asctime)s] [%(name)s:%(lineno)s] [%(threadName)s] %(message)s'
LOGGING_CONFIG = None
LOGGING = {
    'version' : 1,
    'disable_existing_loggers' : False,
    'formatters' : {
        'generic' : {
            'format' : LOG_FORMAT,
        },
        'slack' : {
            'format' : '`' + LOG_FORMAT + '`',
        },
    },
    'handlers' : {
        'stdout' : {
            'class' : 'logging.StreamHandler',
            'formatter' : 'generic',
        },
            },
    'loggers' : {
        '' : {
                        'handlers' : ['stdout'],
                        'level' : 'INFO',
            'propogate' : True,
        },
    },
}
import logging.config
logging.config.dictConfig(LOGGING)


# Application definition
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
        'django_extensions',
        'jdma_control',
        'taggit',
    ]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    ]

ROOT_URLCONF = 'jdma_site.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'jdma_site.wsgi.application'


# Database
DATABASES = {
        'default' : {
                                'ENGINE' : 'django.db.backends.postgresql',
                                            'HOST' : '/tmp',
                                            'ATOMIC_REQUESTS' : True,
                                            'NAME' : 'jdma_control',
                        },
    }


# Authentication settings
AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
LANGUAGE_CODE = 'en-gb'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_L10N = True
USE_TZ = False


# Static files (CSS, JavaScript, Images)
STATIC_URL = '/static/'
STATIC_ROOT = '/var/www/static'


# Email
EMAIL_BACKEND = 'django.core.mail.backends.console.EmailBackend'
SERVER_EMAIL = DEFAULT_FROM_EMAIL = 'jdma-control@jdma-control.ceda.ac.uk'


# Put your custom settings here.
ALLOWED_HOSTS=["192.168.51.26", 
               "192.168.51.26"]
