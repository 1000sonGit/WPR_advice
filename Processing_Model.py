# -*- coding: utf-8 -*-
#from time import process_time, perf_counter

#lxml ← para parsear(fazer a análise sintática)
#  o html e extrair as informações desejadas;

import lxml.html as parser

#requests ← para realizar nossas requisições,
# pode ser substituída por qualquer pacote similar;

import requests
from time import perf_counter
#from time import time as tm
import csv
import pandas as pd
from pandas import DataFrame as df
#import pandas_market_calendars as mark #Precisa do Microsoft Visual Studio C++ instalado
import os.path
import numpy as np
#import sys
#from datetime import date, time, datetime, timedelta
#import monthdelta
import threading
import shelve
#from scipy import interpolate
#from tqdm import tqdm_gui
from tqdm import tqdm
#from random import random, randint
from PyQt5.QtCore import pyqtSignal, QThread
#from PyQt5.QtWidgets import (QApplication, QProgressBar, QWidget)
from collections import Counter, deque
from pulp import *
from functools import reduce
import talib as ta

class StormStrategy(threading.Thread):#threading.Thread, QThread):
    valueChanged = pyqtSignal(int)

    def __init__(self, mm_vol, will_int, mm_will, mutex):

        self.mm_vol = mm_vol
        self.will_int = will_int
        self.mm_will = mm_will
        self.mutex = mutex
        threading.Thread.__init__(self)
        #QThread.__init__(self)

    # Selecionando as cotações
    def run(self):#antigo dadosCotacoes

        # Carregando lista de papéis salvas
        #lista = os.listdir(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_yahoo_CSV')
        #self.listaAcao = [x.replace('.csv', '') for x in lista]# if x[-8] != '1']
        #print(self.listaAcao)

        # Salvando a lista de papéis em arquivo shelve
        ##db = shelve.open('ListaPapeis.db')
        #db['papeis'] = self.listaAcao
        ##self.listaAcao = db['papeis']
        ##db.close()
        #self.listaAcao = ['KNCR11.SA']
        # Utilizando somente a lista das opções com liquidez
        self.listaAcao = ['BOVA11.SA', 'ABEV3.SA', 'BBDC4.SA', 'CIEL3.SA', 'CSNA3.SA', 'GGBR4.SA',
                          'ITUB4.SA', 'ITSA4.SA', 'PETR4.SA', 'USIM5.SA', 'VALE3.SA',
                          'BRFS3.SA', 'SBPS3.SA', 'EMBR3.SA', 'SUZB3.SA', 'MRFG3.SA', 'JBSS3.SA', 'WEGE3.SA',
                          'MGLU3.SA', 'VIIA3.SA', 'COGN3.SA',
                          'B3SA3.SA', 'BPAC11.SA', 'BBSE3.SA', 'CMIG4.SA']
        #print(self.listaAcao)
        self.comprar = []
        self.vender = []


        with tqdm(total=len(self.listaAcao)) as self.pbar:
            with self.mutex:
                for acao in self.listaAcao:
                    #acaoP = acao
                    if os.path.exists(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_yahoo_CSV/{acao}.csv'):
                        Acao = pd.read_csv(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_yahoo_CSV/{acao}.csv',
                                           sep=',',
                                           engine='python')
                        # Removendo os dados nulos
                        Acao.dropna(inplace=True)
                        # Obtendo os dados do volume
                        volume = np.array(Acao.iloc[0:, 6])
                        # Calculando a média do volume
                        media_vol = np.array(ta.MA(Acao.iloc[0:, 6], timeperiod=self.mm_vol, matype=0))
                        # Calculando o William's Percent
                        # WILLR(high, low, close, timeperiod=14)
                        will_perc = np.array(ta.WILLR(Acao.iloc[0:, 2], Acao.iloc[0:, 3], Acao.iloc[0:, 4], timeperiod=self.will_int))
                        # Calculo da média do William's Percent
                        media_will = np.array(ta.EMA(will_perc, timeperiod=self.mm_will))
                        #print(media_will[-3], will_perc[-3])

                        # Condição para compra
                        if(will_perc[-3] < media_will[-3] and will_perc[-1] > media_will[-1] and volume[-1] > 1.05*media_vol[-1] and
                           media_will[-1] < -40):
                            self.comprar.append(acao)

                        elif(will_perc[-3] > media_will[-3] and will_perc[-1] < media_will[-1] and volume[-1] > 1.05*media_vol[-1] and
                           media_will[-1] > -60):
                            self.vender.append(acao)

                    self.pbar.update(1)

        if len(self.comprar) != 0:
            for i in self.comprar:
                print(f'Comprar: {i}')
        if len(self.vender) != 0:
            for r in self.vender:
                print(f'Vender: {r}')


if __name__ == '__main__':
    # Média do volume
    mm_vol = 21
    # Período do indicador
    will_int = 14
    # Média do indicador
    mm_will = 14
    stdoutmutex = threading.Lock()
    threads = []
    obj = StormStrategy(mm_vol, will_int, mm_will, stdoutmutex)
    obj.start()
    threads.append(obj)
    for thread in threads:
        thread.join()
    # Obtendo o tempo de execução:
    duracao = round((perf_counter()), 0)
    horas = int(duracao // 3600)
    minutos = int(round((duracao / 3600 - duracao // 3600) * 60, 0))
    segundos = int(round((duracao % 60), 0))
    print(f'Tempo de execução:{horas}:{minutos}:{segundos}')
    ##############
    #app = QApplication(sys.argv)
    #window = Actions()#meses, kind, descartados, path_old, path_now)#, stdoutmutex)
    #duracao = round((perf_counter()), 0)
    #horas = int(duracao // 3600)
    ##minutos = int(round((duracao / 3600 - duracao // 3600) * 60, 0))
    #segundos = int(round((duracao % 60), 0))
    #print(f'Tempo de execução:{horas}:{minutos}:{segundos}')
    #sys.exit(app.exec_())