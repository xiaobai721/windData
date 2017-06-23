import sys,time,os
import logging, logging.config, logging.handlers

def mylog():
    gLogger = logging.getLogger()
    MAXLOGSIZE = 5 * 1024 * 1024
    logfile = "LogFile/" + time.strftime('%Y-%m-%d',time.localtime(time.time())) + ".log"
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s')
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    gLogger.addHandler(handler)
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s')
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=MAXLOGSIZE)
    handler.setFormatter(formatter)
    gLogger.addHandler(handler)
    gLogger.setLevel(logging.INFO)
    return gLogger


if not os.path.exists(os.getcwd() + '/' + 'LogFile/'):
    os.makedirs(os.getcwd() + '/' + 'LogFile/')
gLogger = mylog()