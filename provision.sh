#!/usr/bin/env bash

sudo pacman -Sy python2-virtualenv
sudo virtualenv2 /home/vagrant/.ve/habcat-api-twisted
sudo chown -R vagrant /home/vagrant/.ve
