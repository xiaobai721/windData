import sys,time,os
import logging, logging.config, logging.handlers
# from cloghandler import ConcurrentRotatingFileHandler

def mylog():
    gLogger = logging.getLogger()
    logfile = "LogFile/" + time.strftime('%Y-%m-%d-%H',time.localtime(time.time())) + ".log"
    # if os.path.exists(logfile) and os.path.getsize(logfile) >= 10000000:
    #     logfile = "LogFile/" + time.strftime('%Y-%m-%d-%H-%M',time.localtime(time.time())) + ".log"
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s')
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    handler.setLevel(logging.WARN)
    gLogger.addHandler(handler)
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s')
    handler = logging.handlers.RotatingFileHandler(logfile)
    handler.setLevel(logging.INFO)
    # handler = ConcurrentRotatingFileHandler(logfile, maxBytes=10, delay=True)
    handler.setFormatter(formatter)
    gLogger.addHandler(handler)
    gLogger.setLevel(logging.INFO)
    return gLogger


if not os.path.exists(os.getcwd() + '/' + 'LogFile/'):
    os.makedirs(os.getcwd() + '/' + 'LogFile/')
gLogger = mylog()