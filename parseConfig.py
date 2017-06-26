#-*- coding: utf-8 -*-
import configparser
import os
# from module_mylog import gLogger

def getConfig(section, key):
    config = configparser.ConfigParser()
    # path = os.path.split(os.path.realpath(__file__))[0] + '/wind.conf'
    path = resource_path("wind.conf")
    # gLogger.info(path)
    config.read(path)
    return config.get(section, key)

def resource_path(relative):
    return os.path.join(
        os.environ.get(
            "_MEIPASS2",
            os.path.abspath(".")
        ),
        relative
    )