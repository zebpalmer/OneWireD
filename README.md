OneWireD
========

Python daemon for reading dallas 1-wire temperature sensors

I've been running this code for over two years on several servers as I have a pretty extensive sensor network 
that is part of my Home Automation AI. A friend asked for this code, so here it is. It was heavily customized to my 
setup but I've simplified it a bunch and pulled out some very specific features that likely no one else would want
to clean it up a bit. Enjoy. 


Setup
-------

You'll need python-ow installed and configured (See owfs.conf.example for suggested settings), then run the program
with the path to the config file. Obviously you'll want to look at the example config file (onewired.cfg.example)
and tailor it as needed. 


