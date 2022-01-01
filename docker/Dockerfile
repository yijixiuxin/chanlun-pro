FROM ubuntu:20.04

MAINTAINER yijixiuxin<1058715329@qq.com>

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG C.UTF-8

SHELL ["/bin/bash", "-c"]

WORKDIR /root

COPY sources.list /etc/apt/
COPY .condarc /root
COPY condainit /root
COPY mysql_secure.sh /root/
COPY mysql.sql /root/
COPY pm2.config.js /root/
COPY start.sh /usr/local/

RUN apt update && apt install -y \
    sudo \
    apt-utils \
    vim \
    git \
    subversion \
    g++ \
    gdb \
    cmake \
    openjdk-11-jdk \
    mysql-server \
    redis \
    openssh-server \
    net-tools \
    iputils-ping \
    telnet \
    lsb-core \
    iptables \
    expect \
    tzdata \
    npm \
    wget && \
    wget https://mirrors.bfsu.edu.cn/anaconda/archive/Anaconda3-2020.02-Linux-x86_64.sh && \
    bash Anaconda3-2020.02-Linux-x86_64.sh -b -p /usr/local/anaconda3/ && \
    rm Anaconda3-2020.02-Linux-x86_64.sh && \
    source /usr/local/anaconda3/bin/activate && \
    conda init && \
    conda clean -i && \
    pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/ && \
    source .bashrc && \
    conda create -y -n cl python=3.7 && \
    conda activate cl && \
    conda install -y pandas requests numpy redis matplotlib pymysql && \
    conda install -y -c conda-forge ta-lib && \
    conda install -y -c conda-forge jupyterlab  && \
    pip3 install django==3.2.9 redis futu-api ccxt prettytable pyttsx3 pyecharts==1.9.1 djangorestframework baostock dbutils && \
    npm install -g pm2 && \
    jupyter-lab --generate-config && \
    conda deactivate && \
    chmod 777 -R /usr/local/anaconda3/ && \
    usermod -d /var/lib/mysql/ mysql && \
    service mysql start && \
    chmod +x /root/mysql_secure.sh && \
    expect /root/mysql_secure.sh && \
    sed -i "s/bind-address.*= 127.0.0.1/bind-address            = 0.0.0.0/g" /etc/mysql/mysql.conf.d/mysqld.cnf && \
    mysql -P 3306 -uroot -p123456 < /root/mysql.sql && \
    service mysql restart && \
    echo "Asia/Shanghai" > /etc/timezone && \
    ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    chmod 777 /usr/local/start.sh && \
    touch /usr/local/start.log && \
    chmod 777 /usr/local/start.log && \
    cd /root && \
    cat condainit >> /etc/profile && \
    echo -e >> /etc/profile && \
    echo "set completion-ignore-case on" >> ~/.inputrc && \
    source /etc/profile && \
    mkdir /root/app

WORKDIR /root/app

ENV DEBIAN_FRONTEND=dialog

VOLUME ["/home/app", "/var/lib/mysql", "/var/lib/redis"]
EXPOSE 3306
EXPOSE 8000
EXPOSE 8888

ENTRYPOINT ["/bin/bash", "/usr/local/start.sh"]