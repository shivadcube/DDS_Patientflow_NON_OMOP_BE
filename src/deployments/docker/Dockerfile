FROM python:3.5.7-alpine3.10

# ensure local python is preferred over distribution python
ENV PATH /usr/local/bin:$PATH

# http://bugs.python.org/issue19846
# > At the moment, setting "LANG=C" on a Linux system *fundamentally breaks Python 3*, and that's not OK.
ENV LANG C.UTF-8

ENV PYTHON_VERSION 3.5.7

# General
RUN apk update

# create user
# RUN addgroup -S dds3 && adduser -S dds3 -G dds3
# RUN echo "dds3 ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Install system packages
RUN apk add --no-cache bash
RUN apk add g++
RUN apk add --virtual .build-deps gcc libffi-dev
RUN apk del libressl-dev
RUN apk add openssl-dev
RUN apk add --no-cache mariadb-dev

# Create DIR(s)
RUN mkdir -p /application

# Copy Code
COPY . /application/
RUN ["/bin/sh", "/application/deployments/install.sh", "/application"]

# CleanUp
RUN apk del openssl-dev
# RUN apk add libressl-dev
RUN apk del .build-deps

# Port and protocal
EXPOSE 80/tcp

# Run Application
ARG ENVIRONMENT
CMD /usr/local/bin/python3 /application/src/manage.py runserver --settings=settings.$ENVIRONMENT 0:80
#CMD ["/usr/local/bin/python3", "/application/src/manage.py", "runserver", "--settings=settings.$ENVIRONMENT_settings", "0:80"]

# Entry
#ENTRYPOINT ["/usr/bin/cd", "/application/"]
