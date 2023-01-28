FROM cluster-base

# -- Layer: JupyterLab

ARG spark_version=3.3.1
ARG jupyterlab_version=3.5.2

COPY ./ ${SHARED_WORKSPACE}/twitter/

RUN apt-get install debian-archive-keyring
RUN wget -O - ports.debian.org/archive_2021.key | apt-key add -
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 

# python3.10
RUN apt-get update --allow-insecure-repositories -y && \
    apt-get install build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev libsqlite3-dev wget libbz2-dev -y && \
    wget https://www.python.org/ftp/python/3.10.9/Python-3.10.9.tgz && \
    tar -xf Python-3.10.*.tgz && \
    cd Python-3.10.*/ && \
    ./configure --enable-optimizations && \
    make -j $(nproc) && \
    make altinstall && \
    cd .. && rm -r Python-3.10.* && \
    python3.10 --version && \
    curl https://bootstrap.pypa.io./get-pip.py | python3.10 && \
    python3.10 -m pip install --upgrade pip
    
RUN python3.10 -m pip install --no-cache-dir pyspark==${spark_version} jupyterlab==${jupyterlab_version}

# custom .whl's
# RUN python3 -m pip install /opt/workspace/redditStreaming/target/reddit-0.1.0-py3-none-any.whl --force-reinstall

# requirements
RUN python3.10 -m pip install --no-cache-dir -r /opt/workspace/twitter/env/requirements.txt --ignore-installed
    
RUN rm -rf /var/lib/apt/lists/*
    # ln -s /usr/local/bin/python3 /usr/bin/python

# deal w/ outdated pyspark guava jar for hadoop-aws (check maven repo for hadoop-common version)
RUN cd /usr/local/lib/python3.10/site-packages/pyspark/jars/ && \
    rm guava-*.jar && \
    wget https://repo1.maven.org/maven2/com/google/guava/guava/31.1-jre/guava-31.1-jre.jar

# -- Runtime

EXPOSE 8888
WORKDIR ${SHARED_WORKSPACE}
CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=
