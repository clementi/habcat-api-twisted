#!/usr/bin/env bash

sudo apt-get install python-virtualenv -y
sudo gem install foreman
sudo virtualenv /home/vagrant/.ve/habcat-api-twisted
sudo chown -R vagrant /home/vagrant/.ve
