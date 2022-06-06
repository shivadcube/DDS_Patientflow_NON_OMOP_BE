import environ
env = environ.Env()
environ.Env.read_env()

#******************************************************************************#
#**** [2017] <XXXXX>
#**** All Rights Reserved.
#******************************************************************************#


# Prod DB CONFIG
#*******************************************************************************#

DEFAULT_PROD_DATABASE_CONFIG = {
    'ENGINE': 'django.db.backends.mysql',
    'NAME': env('DEFAULT_PROD_DATABASE_CONFIG_NAME'),
    'USER': env('DEFAULT_PROD_DATABASE_CONFIG_USER'),
    'PASSWORD': env('DEFAULT_PROD_DATABASE_CONFIG_PASS'),
    'HOST': env('DEFAULT_PROD_DATABASE_CONFIG_HOST'),
    'PORT': env('DEFAULT_PROD_DATABASE_CONFIG_PORT'),
}

READONLY_PROD_DATABASE_CONFIG = {
    'ENGINE': 'django.db.backends.mysql',
    'NAME': env('READONLY_PROD_DATABASE_CONFIG_NAME'),
    'USER': env('READONLY_PROD_DATABASE_CONFIG_USER'),
    'PASSWORD': env('READONLY_PROD_DATABASE_CONFIG_PASS'),
    'HOST': env('READONLY_PROD_DATABASE_CONFIG_HOST'),
    'PORT': env('READONLY_PROD_DATABASE_CONFIG_PORT'),
}

# Staging DB CONFIG, root, wntcowtiqciiwhnuxoiuwer
#*******************************************************************************#

DEFAULT_STAGING_DATABASE_CONFIG = {
    'ENGINE': 'django.db.backends.mysql',
    'NAME': env('DEFAULT_STAGING_DATABASE_CONFIG_NAME'),
    'USER': env('DEFAULT_STAGING_DATABASE_CONFIG_USER'),
    'PASSWORD': env('DEFAULT_STAGING_DATABASE_CONFIG_PASS'),
    'HOST': env('DEFAULT_STAGING_DATABASE_CONFIG_HOST'),
    'PORT': env('DEFAULT_STAGING_DATABASE_CONFIG_PORT'),
}

READONLY_STAGING_DATABASE_CONFIG = {
    'ENGINE': 'django.db.backends.mysql',
    'NAME': env('READONLY_STAGING_DATABASE_CONFIG_NAME'),
    'USER': env('READONLY_STAGING_DATABASE_CONFIG_USER'),
    'PASSWORD': env('READONLY_STAGING_DATABASE_CONFIG_PASS'),
    'HOST': env('READONLY_STAGING_DATABASE_CONFIG_HOST'),
    'PORT': env('READONLY_STAGING_DATABASE_CONFIG_PORT'),
}

# Local DB CONFIG
#*******************************************************************************#

DEFAULT_LOCAL_DATABASE_CONFIG = {
    'ENGINE': 'django.db.backends.mysql',
    'NAME': env('DEFAULT_LOCAL_DATABASE_CONFIG_NAME'),
    'USER': env('DEFAULT_LOCAL_DATABASE_CONFIG_USER'),
    'PASSWORD': env('DEFAULT_LOCAL_DATABASE_CONFIG_PASS'),
    'HOST': env('DEFAULT_LOCAL_DATABASE_CONFIG_HOST'),
    'PORT': env('DEFAULT_LOCAL_DATABASE_CONFIG_PORT'),
}

READONLY_LOCAL_DATABASE_CONFIG = {
    'ENGINE': 'django.db.backends.mysql',
    'NAME': env('READONLY_LOCAL_DATABASE_CONFIG_NAME'),
    'USER': env('READONLY_LOCAL_DATABASE_CONFIG_USER'),
    'PASSWORD': env('READONLY_LOCAL_DATABASE_CONFIG_PASS'),
    'HOST': env('READONLY_LOCAL_DATABASE_CONFIG_HOST'),
    'PORT': env('READONLY_LOCAL_DATABASE_CONFIG_PORT'),
}

#*******************************************************************************#
