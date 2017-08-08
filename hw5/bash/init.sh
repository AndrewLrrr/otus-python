#!/usr/bin/env bash

sudo yum -y install epel-release
sudo yum -y install python-pip python-devel nginx gcc
sudo pip install uwsgi
sudo mv /etc/nginx/nginx.conf /etc/nginx/nginx.conf.backup
sudo cp /home/vagrant/shared/nginx.conf /etc/nginx/nginx.conf
sudo systemctl restart nginx
sudo systemctl enable nginx

sudo sed -i "s/SELINUX=enforcing/SELINUX=disabled/g" /etc/selinux/config
