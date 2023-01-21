FROM cluster-base

# -- Layer: JupyterLab

ARG spark_version=3.3.1
ARG jupyterlab_version=3.5.2

COPY ./env/requirements.txt ${SHARED_WORKSPACE}/twitter/
COPY ./main/ ${SHARED_WORKSPACE}/twitter/

# base python
RUN apt-get install debian-archive-keyring
RUN wget -O - ports.debian.org/archive_2021.key | apt-key add -
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 
# RUN mkdir /apt
# RUN mkdir /apt/etc
# RUN chmod +rwx /apt/etc
# RUN touch /apt/etc/sources.list
# RUN cat "deb http://deb.debian.org/debian/ buster/updates main contrib non-free" >> /apt/etc/sources.list
# RUN cat "deb-src http://deb.debian.org/debian/ buster/updates main contrib non-free" >> /apt/etc/sources.list

RUN apt-get update --allow-insecure-repositories -y && \
    apt-get install build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev libsqlite3-dev wget libbz2-dev -y && \
    wget https://www.python.org/ftp/python/3.10.9/Python-3.10.9.tgz && \
    tar -xf Python-3.10.*.tgz && \
    cd Python-3.10.*/ && \
    ./configure --enable-optimizations && \
    make -j $(nproc) && \
    make altinstall && \
    cd .. && rm -r Python-3.10.* && \
    apt install python3-pip python3-distutils python3-setuptools
    
RUN python -m pip install --no-cache-dir pyspark==${spark_version} jupyterlab==${jupyterlab_version}

# custom .whl's
# RUN python3 -m pip install /opt/workspace/redditStreaming/target/reddit-0.1.0-py3-none-any.whl --force-reinstall

# requirements
RUN python -m pip install --no-cache-dir -r /opt/workspace/twitter/requirements.txt --ignore-installed
    
RUN rm -rf /var/lib/apt/lists/* && \
    mkdir root/.aws
    # ln -s /usr/local/bin/python3 /usr/bin/python

# deal w/ outdated pyspark guava jar for hadoop-aws (check maven repo for hadoop-common version)
RUN ls /usr/local/lib/python3.7/dist-packages/pyspark/jars/

RUN cd /usr/local/lib/python3.7/dist-packages/pyspark/jars/ && \
    rm guava-14.0.1.jar && \
    wget https://repo1.maven.org/maven2/com/google/guava/guava/27.0-jre/guava-27.0-jre.jar

# -- Runtime

EXPOSE 8888
WORKDIR ${SHARED_WORKSPACE}
CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=
