
# -*- coding: utf-8 -*-

import os


# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


DEBUG = True
TESTING = True



# Read the secret key from a file
SECRET_KEY_FILE = '/home/vagrant/JDMA/conf/secret_key.txt'
with open(SECRET_KEY_FILE) as f:
    SECRET_KEY = f.read().strip()


# Logging settings


# Application definition
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
        'django.contrib.postgres',
        'django_extensions',
        'jdma_control',
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


# IMPORTANT: CookieStorage (and hence FallbackStorage, which is the default) interacts
#            badly with Chrome's prefetching, causing messages to be rendered twice
#            or not at all...!
MESSAGE_STORAGE = 'django.contrib.messages.storage.session.SessionStorage'


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
# Email
EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
SERVER_EMAIL = DEFAULT_FROM_EMAIL = 'jdma@jdma.ceda.ac.uk'



# Read the encrypt / decrypt key from a file
ENCRYPT_KEY_FILE = '/home/vagrant/JDMA/conf/encrypt_key.txt'

# Put your custom settings here.
ALLOWED_HOSTS=["192.168.51.26",
               "192.168.51.26"]

# App specific settings file for the jdma_control app
JDMA_LDAP_BASE_USER = "OU=jasmin,OU=People,O=hpc,DC=rl,DC=ac,DC=uk"
JDMA_LDAP_BASE_GROUP = "OU=ceda,OU=Groups,O=hpc,DC=rl,DC=ac,DC=uk"
JDMA_LDAP_PRIMARY = "ldap://homer.esc.rl.ac.uk"
JDMA_LDAP_REPLICAS = ["ldap://marge.esc.rl.ac.uk", "ldap://wiggum.jc.rl.ac.uk"]
