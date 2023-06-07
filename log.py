import logging

logger = logging.getLogger("main")
logger.setLevel(logging.DEBUG)
ch = logging.FileHandler('log.txt')
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %('
                                  'levelname)s - %(message)s'))
logger.addHandler(ch)