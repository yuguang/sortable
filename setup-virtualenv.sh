#!/usr/bin/env bash
pip install virtualenv
virtualenv -p /usr/bin/python3 dev-python
source dev-python/bin/activate
pip install "numpy>=1.9"
pip install dedupe