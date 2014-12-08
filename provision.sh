#!/usr/bin/env bash

sudo apt-get install htop -y
sudo apt-get install python-virtualenv -y
sudo apt-get install python-dev -y
sudo gem install foreman
sudo virtualenv /home/vagrant/.ve/habcat-api-twisted
sudo chown -R vagrant /home/vagrant/.ve
sudo echo "export PORT=5000" >> /home/vagrant/.bashrc
