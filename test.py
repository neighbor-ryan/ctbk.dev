#!/usr/bin/env python
import os

secret = os.environ.get('SECRET1')
if secret:
    print('Found env.SECRET1!')
else:
    print("Didn't find env.SECRET1.")
