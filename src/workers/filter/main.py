import os
import logging
import threading

from common.domain.transaction import Transaction
from common.message_protocol import *
from common.middleware import middleware
import filters

# Id correspondiente a la entidad
ID = int(os.environ["ID"])
# Host del middleware
MOM_HOST = os.environ["MOM_HOST"]
# Corresponde a como esta configurada la entidad, es decir, como filtra las transacciones
# Configuraciones posibles:
#   - "Q1": transaction.amount < 50
#   - "Q5_PF": transaction.format == "Wire" or transaction.format == "ACH"
#   - "Q5_USD": transaction.amount < 1.0
#   - "USD": transaction.currency == "US Dollar"
#   - "DATE": transaction.is_in_date_range(start_date, end_date)
CONFIGURACION = os.environ["CONFIGURACION"]
# Cola de Entrada
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
# Colas de Salida Posibles
GATEWAY_PREFIX = os.environ["GATEWAY_PREFIX"]
FILTER_DATE_QUEUE = os.environ["FILTER_DATE_QUEUE"]
FILTER_Q1_QUEUE = os.environ["FILTER_Q1_QUEUE"]
SUM_Q2_QUEUE = os.environ["SUM_Q2_QUEUE"]
FILTER_Q3_QUEUE = os.environ["FILTER_Q3_QUEUE"]
SCATTER_GATHER_MAPPER_QUEUE = os.environ["SCATTER_GATHER_MAPPER_QUEUE"]
FILTER_Q5_USD_QUEUE = os.environ["FILTER_Q5_USD_QUEUE"]
AGGREGATOR_Q5_QUEUE = os.environ["AGGREGATOR_Q5_QUEUE"]
# Exchanges de Salida Posibles (necesario hacer sharding)
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_Q3_AMOUNT = int(os.environ["SUM_Q3_AMOUNT"])
# Para control en token ring
CONTROL_EXCHANGE = "CONTROL_EXCHANGE_" + CONFIGURACION
