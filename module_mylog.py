import sys,time
import logging, logging.config, logging.handlers

def mylog():
    gLogger = logging.getLogger()
    logfile = "LogFile/" + time.strftime('%Y-%m-%d',time.localtime(time.time())) + ".log"
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s')
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    gLogger.addHandler(handler)
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s')
    handler = logging.handlers.RotatingFileHandler(logfile)
    handler.setFormatter(formatter)
    gLogger.addHandler(handler)
    gLogger.setLevel(logging.INFO)
    return gLogger
gLogger = mylog()