# -*- coding: utf-8 -*-

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

class WebScraping(threading.Thread):#threading.Thread, QThread):
    valueChanged = pyqtSignal(int)

    def __init__(self, vol_acao, vol_fii, cap_op, perda, short, long, mutex, intervalo):

        self.vol_acao = vol_acao
        self.vol_fii = vol_fii
        self.cap_op = cap_op
        self.perda = perda
        self.short = short
        self.long = long
        self.mutex = mutex
        self.intervalo = intervalo
        threading.Thread.__init__(self)
        #QThread.__init__(self)

    # Selecionando as cotações
    def run(self):#antigo dadosCotacoes

        self.dados_fundos= []

        # Carregando lista de papéis salvas
        #lista = os.listdir(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_yahoo_CSV')
        #self.listaAcao = [x.replace('.csv', '') for x in lista]# if x[-8] != '1']
        #print(self.listaAcao)

        # Salvando a lista de papéis em arquivo shelve
        ##db = shelve.open('ListaPapeis.db')
        #db['papeis'] = self.listaAcao
        ##self.listaAcao = db['papeis']
        ##db.close()
        #self.listaAcao = ['PETR4.SA']
        # Utilizando somente a lista das opções com liquidez
        self.listaAcao = ['BOVA11.SA', 'ABEV3.SA', 'BBDC4.SA', 'CIEL3.SA', 'CSNA3.SA', 'GGBR4.SA',
                          'ITUB4.SA', 'ITSA4.SA', 'PETR4.SA', 'USIM5.SA', 'VALE3.SA',
                          'BRFS3.SA', 'SBSP3.SA', 'EMBR3.SA', 'SUZB3.SA', 'MRFG3.SA', 'JBSS3.SA', 'WEGE3.SA',
                          'MGLU3.SA', 'VIIA3.SA', 'COGN3.SA', 'BBAS3.SA',
                          'B3SA3.SA', 'BPAC11.SA', 'BBSE3.SA', 'CMIG4.SA']
        print(self.listaAcao)
        self.preco = []
        compra = []
        vende = []
        Risk_Return_c = []
        Risk_Return_v = []
        investe_c = []
        investe_v = []
        self.comprar = []
        self.vender = []
        perda_c = []
        perda_v = []
        preco_c = []
        preco_v = []
        stop_c = []
        stop_v = []
        names = []
        #Listas para guardar o tipo de Setup utilizado
        candle_f_c = []
        candle_f_v = []
        self.setup = ''

        with tqdm(total=len(self.listaAcao)) as self.pbar:
            with self.mutex:
                for acao in self.listaAcao:
                    #print(acao)
                    if os.path.exists(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_yahoo_CSV/{self.intervalo}/{acao}.csv'):
                        Acao = pd.read_csv(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_yahoo_CSV/{self.intervalo}/{acao}.csv',
                                           sep=',',
                                           engine='python')
                        # Removendo os dados nulos
                        Acao.dropna(inplace=True)
                        # Obtendo os dados do volume
                        volume = np.array(Acao.iloc[0:, 6])
                        volume_21 = volume[-21:].mean()
                        vol_med = np.nanmean(volume)

                        #Obtendo o preço e ordenando do final para o começo
                        preco = np.array(Acao.iloc[::-1, 4])
                        #Obtendo a máxima e a mínima e ordenando do final para o começo
                        phigh = np.array(Acao.iloc[::-1, 2])
                        plow = np.array(Acao.iloc[::-1, 3])
                        # Obtendo o valor das Medias Móveis (obs: pegando o preço do final para o começo)
                        # FAZENDO AS MeDIAS MÓVEIS
                        # ARMAZENANDO AS MeDIAS COM INDICAÇÃO DE ALTA(+) OU BAIXA(-)
                        # Utilizando biblioteca TALIB para calcular a media exponencial (existe um desvio nos valores quando comparados aos do MTQL5)
                        m9 = np.array(ta.EMA(Acao.iloc[0:, 4], timeperiod=self.short))
                        m21 = np.array(ta.EMA(Acao.iloc[0:, 4], timeperiod=self.long))
                        # Invertendo a ordem do array (obs: pegando a media do final para o começo)
                        m9 = m9[::-1]
                        m9 = m9[:4]
                        m21 = m21[::-1]
                        m21 = m21[:4]
                        # Media maior deslocada em 1
                        m21_deslocada = m21[1:]
                        if (m9[0]) < (m9[1]):
                            med_9 = (m9[0]) * -1
                        else:
                            med_9 = m9[0]

                        if m21[0] < m21[1]:
                            med_21 = m21[0] * -1
                        else:
                            med_21 = m21[0]

                        # Loop para certificar tendência de alta ou baixa da "MeDIA" (Utilizado no Setup 9.1)
                        # OBS: Lembrar que a m9 está do fim para o começo!
                        # Indica media decrescente
                        #botton = False
                        # Indica media crescente
                        #top = False
                        # Parte da posição 1 para não pegar a última media
                        # for i in range(1, 5):
                        #
                        #     if (m9[1+i]) <= (m9[i]):
                        #         top = True
                        #     else:
                        #         # Basta um falso para invalidar
                        #         top = False
                        #         break
                        # if ((med_9 < 0) and (top == True)):
                        #     top = True
                        # else:
                        #     top = False
                        # for i in range(1, 5):
                        #
                        #     if (m9[1+i]) >= (m9[i]):
                        #         botton = True
                        #     else:
                        #         botton = False
                        #         break
                        # if ((med_9 > 0) and (botton == True)):
                        #     botton = True
                        # else:
                        #     botton = False
                        # Loop para identificar correção dos preços SETUP PC (Media lenta)
                        # Para correção de alta
                        co_botton = False
                        #Para correção de baixa
                        co_top = False
                        for i in range(0, 2):

                            if (preco[1 + i]) <= (preco[i]):
                                co_top = True
                            else:
                                co_top = False
                                break
                        if ((med_21 < 0) and (co_top == True)):
                            co_top = True
                        else:
                            co_top = False
                        for i in range(0, 2):

                            if (preco[1 + i]) >= (preco[i]):
                                co_botton = True
                            else:
                                co_botton = False
                                break
                        if ((med_21 > 0) and (co_botton == True)):
                            co_botton = True
                        else:
                            co_botton = False
                        # OBTENDO OS DADOS DOS CANDLES COM USO DOS DADOS DAS MeDIAS
                        # Laço para obter o tamanho dos candles (Fazendo o módulo do tamanho do candle)
                        tam = [(((Acao.iloc[u, 4] - Acao.iloc[u, 1]) ** 2) ** 0.5) for u in range(0, len(volume))]
                        '''
                        for u in range(0, len(volume)):
                            # Fazendo o módulo do tamanho do candle
                            tam.append(((Acao.iloc[u, 4] - Acao.iloc[u, 1]) ** 2) ** 1 / 2)
                            0'Date', 1'Open', 2'High', 3'Low', 4'Close', 5'Variation', 6'Volume'
                        '''
                        tamanho = np.array(tam)
                        tam_med = np.nanmean(tamanho)
                        candle_force = 0
                        #Condições para o Gritos do Mercado
                        # Condição para candle grande VERDE
                        if (tamanho[-1] >= 1.5*tam_med) and (med_9 > 0) and (med_21 > 0) and (m21[0] > m21_deslocada[0]) and ((Acao.iloc[-1, 4] - Acao.iloc[-1, 1]) > 0) and\
                                ((Acao.iloc[-1, 4] - Acao.iloc[-1, 1])/10 >= (Acao.iloc[-1, 2] - Acao.iloc[-1, 4])):
                            candle_force = 1
                        # Condição para candle grande VERMELHO
                        elif (tamanho[-1] >= 1.5*tam_med) and (med_9 < 0) and (med_21 < 0) and (m21[0] < m21_deslocada[0]) and ((Acao.iloc[-1, 4] - Acao.iloc[-1, 1]) < 0) and \
                                ((Acao.iloc[-1, 1] - Acao.iloc[-1, 4])/10 >= (Acao.iloc[-1, 4] - Acao.iloc[-1, 3])):
                            candle_force = 2
                        else:
                            pass
                        # Condição para verificar se a 'sombra' demonstra grito
                            # Primeiro termo da condição abertura e low (candle verde com sombra grande abaixo ou sombra pequena abaixo com candle grande: candle_force = 3)
                            # Segundo termo da condição fechamento e high (candle verde visto como "vermelho" devido sombra grande acima do fechamento: candle_force = 4)
                            # Terceiro termo da condição fechamento e low (candle vermelho visto como "verde" devido sombra grande abaixo do fechamento: candle_force = 5) obs:última condição é p/ ter o mínimo de sombra
                            # Quarto termo da condição abertura e high (candle vermelho com sombra grande acima ou sombra pequena acima com candle grande: candle_force = 6) obs:última condição é p/ ter o mínimo de sombra
                        if ((Acao.iloc[-1, 4] - Acao.iloc[-1, 1]) > 0) and ((Acao.iloc[-1, 4] - Acao.iloc[-1, 3]) >= 1.5*tam_med) and\
                                (med_9 > 0) and (med_21 > 0) and (m21[0] > m21_deslocada[0]) and ((Acao.iloc[-1, 4] - Acao.iloc[-1, 1])/10 >= ((Acao.iloc[-1, 2] - Acao.iloc[-1, 4]))): #((Acao.iloc[-1, 4] - Acao.iloc[-1, 1])*5 <= ((Acao.iloc[-1, 1] - Acao.iloc[-1, 3])))
                            candle_force = 3
                        elif ((Acao.iloc[-1, 4] - Acao.iloc[-1, 1]) > 0) and ((Acao.iloc[-1, 4] - Acao.iloc[-1, 1])*5 <= ((Acao.iloc[-1, 2] - Acao.iloc[-1, 4]))) and\
                                (med_9 < 0) and (med_21 < 0) and (m21[0] < m21_deslocada[0]) and ((Acao.iloc[-1, 4] - Acao.iloc[-1, 1])/10 >= ((Acao.iloc[-1, 1] - Acao.iloc[-1, 3]))):
                            candle_force = 4
                        elif ((Acao.iloc[-1, 4] - Acao.iloc[-1, 1]) < 0) and ((Acao.iloc[-1, 1] - Acao.iloc[-1, 4])*5 <= ((Acao.iloc[-1, 4] - Acao.iloc[-1, 3]))) and\
                                (med_9 > 0) and (med_21 > 0) and (m21[0] > m21_deslocada[0]) and ((Acao.iloc[-1, 4] - Acao.iloc[-1, 1])/10 >= ((Acao.iloc[-1, 2] - Acao.iloc[-1, 1]))):
                            candle_force = 5
                        elif ((Acao.iloc[-1, 4] - Acao.iloc[-1, 1]) < 0) and (1.5*tam_med <= ((Acao.iloc[-1, 2] - Acao.iloc[-1, 4]))) and\
                                (med_9 < 0) and (med_21 < 0) and (m21[0] < m21_deslocada[0]) and ((Acao.iloc[-1, 1] - Acao.iloc[-1, 4])/10 >= ((Acao.iloc[-1, 4] - Acao.iloc[-1, 3]))):#((Acao.iloc[-1, 1] - Acao.iloc[-1, 4])*5 <= ((Acao.iloc[-1, 2] - Acao.iloc[-1, 1])))
                            candle_force = 6
                        # Condição para verificar se a 'sombra' demonstra reversão
                            # Primeiro termo da condição abertura e low (candle verde com sombra grande abaixo: candle_force = 7)
                            # Segundo termo da condição fechamento e high (candle verde visto como "vermelho" devido sombra grande acima do fechamento: candle_force = 8)
                            # Terceiro termo da condição fechamento e low (candle vermelho visto como "verde" devido sombra grande abaixo do fechamento: candle_force = 9) obs:última condição é p/ ter o mínimo de sombra
                            # Quarto termo da condição abertura e high (candle vermelho com sombra grande acima: candle_force = 10) obs:última condição é p/ ter o mínimo de sombra
                        elif ((Acao.iloc[-1, 4] - Acao.iloc[-1, 1]) > 0) and ((Acao.iloc[-1, 4] - Acao.iloc[-1, 1])*5 <= ((Acao.iloc[-1, 1] - Acao.iloc[-1, 3]))) and\
                                (med_9 < 0) and (med_21 < 0) and (m21[0] < m21_deslocada[0]) and ((Acao.iloc[-1, 4] - Acao.iloc[-1, 1])/10 >= (Acao.iloc[-1, 2] - Acao.iloc[-1, 4])):
                            candle_force = 7
                        elif ((Acao.iloc[-1, 4] - Acao.iloc[-1, 1]) > 0) and ((Acao.iloc[-1, 4] - Acao.iloc[-1, 1])*5 <= ((Acao.iloc[-1, 2] - Acao.iloc[-1, 4]))) and\
                                (med_9 > 0) and (med_21 > 0) and (m21[0] > m21_deslocada[0]) and ((Acao.iloc[-1, 4] - Acao.iloc[-1, 1])/10 >= ((Acao.iloc[-1, 1] - Acao.iloc[-1, 3]))):
                            candle_force = 8
                        elif ((Acao.iloc[-1, 4] - Acao.iloc[-1, 1]) < 0) and ((Acao.iloc[-1, 1] - Acao.iloc[-1, 4])*5 <= ((Acao.iloc[-1, 4] - Acao.iloc[-1, 3]))) and\
                                (med_9 < 0) and (med_21 < 0) and (m21[0] < m21_deslocada[0]) and ((Acao.iloc[-1, 1] - Acao.iloc[-1, 4])/10 >= ((Acao.iloc[-1, 2] - Acao.iloc[-1, 1]))):
                            candle_force = 9
                        elif ((Acao.iloc[-1, 4] - Acao.iloc[-1, 1]) < 0) and ((Acao.iloc[-1, 1] - Acao.iloc[-1, 4])*5 <= ((Acao.iloc[-1, 2] - Acao.iloc[-1, 1]))) and\
                                (med_9 > 0) and (med_21 > 0) and (m21[0] > m21_deslocada[0]) and ((Acao.iloc[-1, 1] - Acao.iloc[-1, 4])/10 >= ((Acao.iloc[-1, 4] - Acao.iloc[-1, 3]))):
                            candle_force = 10
                        # Condições para verificar os Setup's 9.1, 9.2 e 9.3
                            # Setup 9.1 compra: Inversão da media para cima: candle_force = 11
                            # Setup 9.1 venda: Inversão da media para baixo: candle_force = 12

                            # Setup 9.2 compra: Correção da tendência de alta: candle_force = 11
                            # Setup 9.2 venda: Correção da tendência de baixa: candle_force = 12
                            # Setup 9.3 compra: Correção da tendência de alta: candle_force = 13
                            # Setup 9.3 venda: Correção da tendência de baixa: candle_force = 14
                            # Cruzamento da media longa compra: candle_force = 15
                            # Cruzamento da media curta venda: candle_force = 16
                            # Setup PC compra: Correção da tendência de alta pela media longa: candle_force = 17
                            # Setup PC venda: Correção da tendência de baixa pela media longa: candle_force = 18
                        #0'Date', 1'Open', 2'High', 3'Low', 4'Close', 5'Variation', 6'Volume'
                        # # Condição para Setup 9.1 Compra
                        # elif ((med_9 > 0) and (botton == True)):
                        #     candle_force = 11
                        # # Condição para Setup 9.1 Venda
                        # elif ((med_9 < 0) and (top == True)):
                        #     candle_force = 12
                        # Condição para Setup 9.2 Compra
                        elif ((med_9 > 0) and (Acao.iloc[-2, 3] > Acao.iloc[-1, 4]) and
                                (Acao.iloc[-2, 4] > Acao.iloc[-3, 4])):
                            candle_force = 11
                        # Condição para Setup 9.2 Venda
                        elif ((med_9 < 0) and (Acao.iloc[-2, 2] < Acao.iloc[-1, 4]) and
                                (Acao.iloc[-2, 4] < Acao.iloc[-3, 4])):
                            candle_force = 12
                        # Condição para Setup 9.3 Compra
                        elif ((med_9 > 0) and (Acao.iloc[-3, 4] > Acao.iloc[-1, 4]) and
                                (Acao.iloc[-3, 4] > Acao.iloc[-2, 4]) and
                                (Acao.iloc[-3, 3] < Acao.iloc[-2, 4]) and (Acao.iloc[-3, 4] > Acao.iloc[-4, 4])):
                            candle_force = 13
                        # Condição para Setup 9.3 Venda
                        elif ((med_9 < 0) and (Acao.iloc[-3, 4] < Acao.iloc[-1, 4]) and
                              (Acao.iloc[-3, 4] < Acao.iloc[-2, 4]) and (Acao.iloc[-3, 2] > Acao.iloc[-2, 4]) and
                              (Acao.iloc[-3, 4] < Acao.iloc[-4, 4])):
                            candle_force = 14
                        # Condição para cruzamento das medias longas Compra
                        elif (m21[0] > m21_deslocada[0] and (m21[2] < m21_deslocada[2] or m21[1] < m21_deslocada[1])  and med_9 > 0):
                            candle_force = 15
                        # Condição para cruzamento das medias longas Venda
                        elif (m21[0] < m21_deslocada[0] and (m21[2] > m21_deslocada[2] or m21[1] > m21_deslocada[1]) and med_9 < 0):
                            candle_force = 16
                        # Condição para Setup PC Compra
                        elif ((med_21 > 0) and (co_botton == True)):
                            candle_force = 17
                        # Condição para Setup PC Venda
                        elif ((med_21 < 0) and (co_top == True)):
                            candle_force = 18
                        else:
                            pass
                        ###Portanto candle_force(ímpar)=VERDE e candle_force(par)=VERMELHO###
                        # Condição para verificar o volume médio do papel e se satisfaz o volume do grito para ações
                        if (vol_med >= self.vol_acao) and (volume[-1] >= 1.3*volume_21) and (candle_force <= 10) and (candle_force != 0) and (acao[4] == '3' or acao[4] == '4' or acao[4] == '5'):

                            names.append(acao)
                            # COLETANDO DADOS PARA VERIFICAR AS REGIÕES (PARA ROMPIMENTO)

                            # Armazenando os picos de altos e baixos
                            alto = []
                            baixo = []
                            # Condição para Candle Verde (Rompimento)
                            if (candle_force % 2 != 0) and (candle_force < 7):
                                for i in range(0, len(volume)-2):
                                    if (preco[i + 1] > preco[i]) and (preco[i + 1] > preco[i + 2]):
                                        alto.append(round(preco[i + 1], 2))
                                    elif (preco[i + 1] < preco[i]) and (preco[i + 1] < preco[i + 2]):
                                        baixo.append(round(preco[i + 1], 2))
                                    else:
                                        pass
                                p_altos = Counter(alto)
                                ascend = 0
                                # Condição para verificar fundos ascendentes
                                if (baixo[0] > baixo[1]) and (baixo[0] > baixo[2]) and (baixo[1] > baixo[2]):
                                    ascend = 1
                                elif (baixo[0] > baixo[1]):
                                    ascend = 2
                                else:
                                    pass
                                #p_baixos = Counter(baixo)
                                #print(p_altos)
                                # Condição para fundos ascendentes, verificar rompimento de topo anterior e apoio nas medias (desvio de 2% das medias)
                                if (preco[0] > alto[0]*1.01) and \
                                        (((Acao.iloc[-1, 1] >= 1.02*med_9) or(Acao.iloc[-1, 1] <= 1.02*med_9)) or ((Acao.iloc[-1, 1] >= 1.02*med_21) or (Acao.iloc[-1, 1] <= 1.02*med_21))):
                                    for z, r in enumerate(alto):#(baixo[0] > baixo[1]) and (baixo[0] > baixo[2]) and (baixo[1] > baixo[2]) and
                                        # Condição para encontrar o topo anterior(verificando sua frequência) e estabelecer o Risco x Retorno
                                        if r > preco[0] and p_altos[r] >= 1:
                                            const = 1
                                            # Condição de tolerância de relação RiscoxRetorno em relação a fundos ascendentes
                                            if ascend == 1:
                                                const = 0.8
                                            elif ascend == 2:
                                                const = 0.85
                                            else:
                                                pass
                                            # Condição para relação Risco x Retorno (Mínimo 1 x 1)
                                            if ((r - preco[0]) >= const*(preco[0] - med_21)):# and (z == 0):#substitui baixo[0] por med_21
                                                compra.append(acao.replace('.SA', ''))
                                                Risk_Return_c.append((r - preco[0])/(preco[0] - med_21))
                                                # Cálculo da porcentagem de perda (caso ocorra)
                                                self.porcentagem = (med_21 / preco[0])
                                                # Cálculo do valor a ser investido
                                                valor = round((self.perda * 100) / ((1 - self.porcentagem) * 100), 2)
                                                if valor > self.cap_op:
                                                    investe_c.append(self.cap_op)
                                                else:
                                                    investe_c.append(round((self.perda * 100) / ((1 - self.porcentagem) * 100), 2))
                                                perda_c.append(round((1-self.porcentagem)*100, 2))
                                                preco_c.append(preco[0])
                                                stop_c.append(med_21)
                                                candle_f_c.append(candle_force)
                                                self.pbar.update(1)
                                                break
                                            # Se o primeiro topo superior encontrado não satisfazer a relação Risco x Retorno, sai do laço com break
                                            else:
                                                self.pbar.update(1)
                                                break
                                        else:
                                            pass
                                else:
                                    pass

                            # Condição para Candle Verde (Reversão pra cima) candles 7 e 9
                            elif (candle_force % 2 != 0) and (candle_force >= 7):
                                # Condição para relação Risco x Retorno (Mínimo 1 x 1), diferença entre os candles de reversão e 7% de distância da media de 9 p/ o fechamento
                                if (((med_9*-1) - preco[0]) >= 1 * (((preco[0] - preco[1])**2)**0.5)) and (preco[0] >= preco[1]) and (preco[0] != preco[1]) and\
                                    (Acao.iloc[-1, 4]*1.07 <= med_9):#(preco[0] - preco[1])):  # substitui baixo[0] por med_21
                                    #valordif = ((med_9*-1) - preco[0])
                                    #vlorinf = ((preco[0] - preco[1])**2)**0.5
                                    compra.append(acao.replace('.SA', ''))
                                    Risk_Return_c.append(((med_9*-1) - preco[0]) / (((preco[0] - preco[1])**2)**0.5))
                                    # Cálculo da porcentagem de perda (caso ocorra)
                                    self.porcentagem = (preco[0] / preco[1])
                                    investe_c.append(round((self.perda * 100) / ((1 - self.porcentagem) * 100), 2))
                                    perda_c.append(round((1 - self.porcentagem) * 100, 2))
                                    preco_c.append(preco[0])
                                    stop_c.append(preco[1])
                                    candle_f_c.append(candle_force)
                                    self.pbar.update(1)
                                # Condição para preço de fechamento igual (Calculando o risco pela diferença entre preço e o low anterior)
                                elif (((med_9 * -1) - preco[0]) >= 1 * ((((Acao.iloc[len(tam) - 2, 3]) - preco[1]) ** 2) ** 0.5)) and (preco[0] >= (Acao.iloc[len(tam) - 2, 3])) and\
                                    (Acao.iloc[-1, 4]*1.07 <= med_9):  # substitui baixo[0] por med_21 (Atenção med_9 POSITIVO)
                                    compra.append(acao.replace('.SA', ''))
                                    Risk_Return_c.append(((med_9 * -1) - preco[0]) / (((preco[0] - (Acao.iloc[len(tam) - 2, 3])) ** 2) ** 0.5))
                                    # Cálculo da porcentagem de perda (caso ocorra)
                                    self.porcentagem = (preco[0] / (Acao.iloc[len(tam) - 2, 3]))
                                    investe_c.append(round((self.perda * 100) / ((1 - self.porcentagem) * 100), 2))
                                    perda_c.append(round((1 - self.porcentagem) * 100, 2))
                                    preco_c.append(preco[0])
                                    stop_c.append((Acao.iloc[len(tam) - 2, 3]))
                                    candle_f_c.append(candle_force)
                                    self.pbar.update(1)
                                # Se não satisfazer a relação Risco x Retorno, passa
                                else:
                                    pass

                            # Condição para Candle Vermelho (Rompimento)
                            elif (candle_force % 2 == 0) and (candle_force < 7):# Candle Vermelho
                                for i in range(0, len(volume)-2):
                                    # Condição para coletar os topos (aqui chamados de baixo)
                                    if (preco[i + 1] > preco[i]) and (preco[i + 1] > preco[i + 2]):
                                        baixo.append(round(preco[i + 1], 2))
                                    # Condição para coletar os fundos (aqui chamados de alto)
                                    elif (preco[i + 1] < preco[i]) and (preco[i + 1] < preco[i + 2]):
                                        alto.append(round(preco[i + 1], 2))
                                    else:
                                        pass
                                descend = 0
                                # Condição para verificar fundos ascendentes
                                if (alto[0] > alto[1]) and (alto[0] > alto[2]) and (alto[1] > alto[2]):
                                    descend = 1
                                elif (alto[0] > alto[1]):
                                    descend = 2
                                else:
                                    pass
                                #p_altos = Counter(alto)
                                p_baixos = Counter(baixo)
                                # Condição para topos descendentes, verificar rompimento de fundo anterior e apoio nas medias (desvio de 2% das medias)
                                if (preco[0] < baixo[0]*0.99) and \
                                        (((Acao.iloc[-1, 1] >= 1.02 * (med_9*-1)) or (Acao.iloc[-1, 1] <= 1.02 * (med_9*-1))) or (
                                                (Acao.iloc[-1, 1] >= 1.02 * (med_21*-1)) or (
                                                Acao.iloc[-1, 1] <= 1.02 * (med_21*-1)))):#(alto[0] > alto[1]) and (alto[0] > alto[2]) and (alto[1] > alto[2]) and
                                    for z, r in enumerate(baixo):
                                        # Condição para encontrar o fundo anterior(verificando sua frequência) e estabelecer o Risco x Retorno
                                        if r < preco[0] and p_baixos[r] >= 1:
                                            const = 1
                                            # Condição de tolerância de relação RiscoxRetorno em relação a fundos ascendentes
                                            if descend == 1:
                                                const = 0.8
                                            elif descend == 2:
                                                const = 0.85
                                            else:
                                                pass
                                            # Condição para relação Risco x Retorno (Mínimo 1 x 1)
                                            if ((r - preco[0])*-1 >= -const * (preco[0] - (med_21*-1))):# and (z == 0): #Substitui alto[0] por med_21
                                                vende.append(acao.replace('.SA', ''))
                                                Risk_Return_v.append((r - preco[0])*-1 / (preco[0] - (med_21*-1))*-1)
                                                # Cálculo da porcentagem de perda (caso ocorra)
                                                self.porcentagem = (preco[0]/(med_21*-1))
                                                # Cálculo do valor a ser investido
                                                valor = round((self.perda * 100) / ((1 - self.porcentagem) * 100), 2)
                                                if valor > self.cap_op:
                                                    investe_v.append(self.cap_op)
                                                else:
                                                    investe_v.append(round((self.perda * 100) / ((1 - self.porcentagem) * 100), 2))
                                                perda_v.append(round((1-self.porcentagem)*100, 2))
                                                preco_v.append(preco[0])
                                                stop_v.append((med_21*-1))
                                                candle_f_v.append(candle_force)
                                                self.pbar.update(1)
                                                break
                                            # Se o primeiro topo superior encontrado não satisfazer a relação Risco x Retorno, sai do laço com break
                                            else:
                                                self.pbar.update(1)
                                                break
                                        else:
                                            pass
                                else:
                                    pass

                            # Condição para Candle Vermelho (reversão pra baixo) candles 8 e 10
                            elif (candle_force % 2 == 0) and (candle_force >= 7):
                                # Condição para relação Risco x Retorno (Mínimo 1 x 1), diferença entre os candles de reversão e 7% de distância da media de 9 p/ o fechamento
                                if ((preco[0] - med_9) >= 1 * (preco[1] - preco[0])) and (preco[0] <= preco[1]) and (preco[0] != preco[1]) and\
                                    (Acao.iloc[-1, 4]*1.07 >= med_9):  # substitui baixo[0] por med_9
                                    vende.append(acao.replace('.SA', ''))
                                    Risk_Return_v.append((preco[0] - med_9) / (preco[1] - preco[0]))
                                    # Cálculo da porcentagem de perda (caso ocorra)
                                    self.porcentagem = (preco[1] / preco[0])
                                    investe_v.append(round((self.perda * 100) / ((1 - self.porcentagem) * 100), 2))
                                    perda_v.append(round((self.porcentagem) * 100, 2))
                                    preco_v.append(preco[0])
                                    stop_v.append(preco[1])
                                    candle_f_v.append(candle_force)
                                    self.pbar.update(1)
                                # Condição para preço de fechamento igual (Calculando o risco pela diferença entre preço e o high anterior)
                                elif ((preco[0] - med_9) >= 1 * (Acao.iloc[len(tam)-2, 2] - preco[0])) and (preco[0] <= Acao.iloc[len(tam)-2, 2]) and\
                                    (Acao.iloc[-1, 4]*1.07 >= med_9):  # substitui baixo[0] por med_9
                                    vende.append(acao.replace('.SA', ''))
                                    Risk_Return_v.append((preco[0] - med_9) / (Acao.iloc[len(tam)-2, 2] - preco[0]))
                                    # Cálculo da porcentagem de perda (caso ocorra)
                                    self.porcentagem = ((Acao.iloc[len(tam)-2, 2] - preco[0]) / preco[0])
                                    investe_v.append(round((self.perda * 100) / ((1 - self.porcentagem) * 100), 2))
                                    perda_v.append(round((self.porcentagem) * 100, 2))
                                    preco_v.append(preco[0])
                                    stop_v.append(Acao.iloc[len(tam)-2, 2]) #preco[1]
                                    candle_f_v.append(candle_force)
                                    self.pbar.update(1)

                                # Se não satisfazer a relação Risco x Retorno, passa
                                else:
                                    pass
                            else:
                                pass
                            #((dadosAcao.iloc[u, 4] - dadosAcao.iloc[u, 1]) ** 2) ** 1/2
                            # INSERINDO O RISCO
                            # CONVERTENDO A VARIAÇÃO(f(x)) PARA ARRAY
                            #varia = [float(x) for x in self.variacao]
                            #var = array(varia)
                            # CALCULANDO A MeDIA DA FUNÇÃO
                            #media_func = var.mean()
                            # OBTENDO A MeDIA DO INTERVALO
                            #inter = [x for x in range(1, len(self.variacao) + 1)]
                            #media_inter = array(inter).mean()
                            # OBTENDO A FUNÇÃO DA MeDIA
                            #radFunc = interpolate.Rbf(inter, var, function='gaussian')
                            #funcMean = radFunc(media_inter)
                            # Inserindo o "Risco"
                            #self.unilist.append(funcMean / media_func)

                        # Condição para verificar se o candle_force representa um Setup
                        # 0'Date', 1'Open', 2'High', 3'Low', 4'Close', 5'Variation', 6'Volume'
                        elif ((candle_force > 10) and (candle_force != 0)):
                            names.append(acao)
                            # Armazenando os picos de altos e baixos
                            alto = []
                            baixo = []

                            # Condição para Setup 9.2, 9.3, Cruzamento Compra, PC

                            if ((candle_force == 11) or (candle_force == 13) or (candle_force == 15) or (candle_force == 17)):
                                # Loop para coletar os topos e fundos
                                for i in range(0, len(volume)-2):
                                    if (phigh[i + 1] > phigh[i]) and (phigh[i + 1] > phigh[i + 2]):
                                        alto.append(round(phigh[i + 1], 2))
                                    elif (plow[i + 1] < plow[i]) and (plow[i + 1] < plow[i + 2]):
                                        baixo.append(round(plow[i + 1], 2))
                                    else:
                                        pass
                                p_altos = Counter(alto)
                                # Condição para relação Risco x Retorno (Mínimo 1 x 1)
                                favoravel = Acao.iloc[-1, 2] + 0.01 #Expressa o preço de entrada
                                for z, r in enumerate(alto):  # (baixo[0] > baixo[1]) and (baixo[0] > baixo[2]) and (baixo[1] > baixo[2]) and
                                    # Condição para encontrar o topo anterior(verificando sua frequência) e estabelecer o Risco x Retorno
                                    if r > preco[0] and p_altos[r] >= 1:
                                        Retorno = r - favoravel
                                        Risco = favoravel - (Acao.iloc[-1, 3])
                                        if (Retorno >= Risco):
                                            compra.append(acao.replace('.SA', ''))
                                            Risk_Return_c.append(Retorno / Risco)
                                        # Cálculo da porcentagem de perda (caso ocorra)
                                            self.porcentagem = (Risco / favoravel)
                                        # Cálculo do valor a ser investido
                                            valor = round((self.perda * 100) / ((1 - self.porcentagem) * 100), 2)
                                            if valor > self.cap_op:
                                                investe_c.append(self.cap_op)
                                            else:
                                                investe_c.append(round((self.perda * 100) / ((1 - self.porcentagem) * 100), 2))
                                            perda_c.append(round((1 - self.porcentagem) * 100, 2))
                                            preco_c.append(favoravel)
                                            stop_c.append(Acao.iloc[-1, 3])
                                            candle_f_c.append(candle_force)
                                            self.pbar.update(1)
                                            break
                                        # Se o primeiro topo superior encontrado não satisfazer a relação Risco x Retorno, sai do laço com break
                                        else:
                                            self.pbar.update(1)
                                            break
                                    else:
                                        pass


                            # Condição para Setup 9.2, 9.3, Cruzamento Venda, PC

                            elif ((candle_force == 12) or (candle_force == 14) or (candle_force == 16) or (candle_force == 18)):
                                # Loop para coletar os topos e fundos
                                for i in range(0, len(volume) - 2):
                                    if (phigh[i + 1] > phigh[i]) and (phigh[i + 1] > phigh[i + 2]):
                                        alto.append(round(phigh[i + 1], 2))
                                    elif (plow[i + 1] < plow[i]) and (plow[i + 1] < plow[i + 2]):
                                        baixo.append(round(plow[i + 1], 2))
                                    else:
                                        pass
                                p_baixos = Counter(baixo)
                                favoravel = Acao.iloc[-1, 3] - 0.01  # Expressa o preço de entrada
                                for z, r in enumerate(baixo):
                                    # Condição para encontrar o fundo anterior(verificando sua frequência) e estabelecer o Risco x Retorno
                                    if r < favoravel and p_baixos[r] >= 1:
                                        # Condição para relação Risco x Retorno (Mínimo 1 x 1)
                                        Retorno = favoravel - r
                                        Risco = (Acao.iloc[-1, 2]) - favoravel
                                        if (Retorno >= Risco):
                                            vende.append(acao.replace('.SA', ''))
                                            Risk_Return_v.append(Retorno / Risco)
                                            # Cálculo da porcentagem de perda (caso ocorra)
                                            self.porcentagem = (Risco / favoravel)
                                            # Cálculo do valor a ser investido
                                            valor = round((self.perda * 100) / ((1 - self.porcentagem) * 100), 2)
                                            if valor > self.cap_op:
                                                investe_v.append(self.cap_op)
                                            else:
                                                investe_v.append(
                                                    round((self.perda * 100) / ((1 - self.porcentagem) * 100), 2))
                                            perda_v.append(round((1 - self.porcentagem) * 100, 2))
                                            preco_v.append(favoravel)
                                            stop_v.append(Acao.iloc[-1, 2])
                                            candle_f_v.append(candle_force)
                                            self.pbar.update(1)
                                            break
                                        # Se o primeiro topo superior encontrado não satisfazer a relação Risco x Retorno, sai do laço com break
                                        else:
                                            self.pbar.update(1)
                                            break

                                    else:
                                        pass

                            else:
                                pass

                        else:

                            self.pbar.update(1)
                            #self.valueChanged.emit(Count)
                            continue

                    else:
                        self.pbar.update(1)
                        #self.valueChanged.emit(Count)
                        pass

        setup_dic = {1: 'Grito de compra', 2: 'Grito de venda', 3: 'Grito de compra', 4: 'Grito de venda',
                     5: 'Grito de compra', 6: 'Grito de venda', 7: 'Grito de compra', 8: 'Grito de venda',
                     9: 'Grito de compra', 10: 'Grito de venda', 11: 'Setup 9.2 compra', 12: 'Setup 9.2 venda',
                     13: 'Setup 9.3 compra', 14: 'Setup 9.3 venda', 15: 'Cruzamento da media longa compra',
                     16: 'Cruzamento da media longa venda', 17: 'Setup PC compra', 18: 'Setup PC venda'}
        for c, i in enumerate(compra):
            # print(f'#####\nCompra: {i}\nRisco x Retorno: {Risk_Return_c[c]}\nInveste: {investe_c[c]}\n#####')
            qnt_invest = 100 * ((round((investe_c[c] / preco_c[c]), 0)) // 100)
            valor_invest = qnt_invest * round(preco_c[c], 2)
            self.comprar.append(['Comprar', i, round(Risk_Return_c[c], 2), valor_invest, perda_c[c], round(preco_c[c], 2), qnt_invest, round(stop_c[c], 2), setup_dic[candle_f_c[c]]])

        for c, i in enumerate(vende):
            # print(f'#####\nVende: {i}\nRisco x Retorno: {Risk_Return_v[c]}\nInveste: {investe_v[c]}\n#####')
            qnt_invest = 100 * ((round((investe_v[c] / preco_v[c]), 0)) // 100)
            valor_invest = qnt_invest * round(preco_v[c], 2)
            self.vender.append(['Vender', i, round(Risk_Return_v[c], 2), valor_invest, perda_v[c], round(preco_v[c], 2), qnt_invest, round(stop_v[c], 2), setup_dic[candle_f_v[c]]])

        if len(self.comprar) != 0:
            for i in self.comprar:
                print(f'Comprar: {i}')
        if len(self.vender) != 0:
            for r in self.vender:
                print(f'Vender: {r}')

    def dadosSelect(self):
        # Verificando se o arquivo existe
        if os.path.exists(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_Resultado/Dados.csv'):
            os.remove(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_Resultado/Dados.csv')
        with open(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_Resultado/Dados.csv', 'w', newline='') as dados:
            writer = csv.writer(dados)
            col_name = ['Operation', 'Papel', 'Risco x Retorno', 'Qtd a Investir', '% de Perda', 'Price', 'Qtd de papel', 'Stop loss', 'Setup']
            #print(col_name)
            writer.writerow(col_name)
            for d in self.comprar:
                writer.writerow(d)
            for d in self.vender:
                writer.writerow(d)
        # Lendo a tabela com PANDAS Dataframe
        df = pd.read_csv('E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_Resultado/Dados.csv', engine='python')
        print(df)

if __name__ == '__main__':
    vol_acao = 8000.0
    vol_fii = 30000.0
    # Parâmetros de para determinar o ganho mensal bem como o de risco(perda)
    capital_op = 20000.0
    perda = 800.0
    # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
    # (optional, default is '1d')
    interval = "1wk"
    #taxa_mensal = 1.5/100
    #cap_semanal = perda + capital_op*taxa_mensal/4
    #taxa_semanal = (cap_semanal/perda) - 1
    #print(taxa_semanal)
    short = 9
    long = 21
    stdoutmutex = threading.Lock()
    threads = []
    obj = WebScraping(vol_acao, vol_fii, capital_op, perda, short, long, stdoutmutex, interval)
    obj.start()
    threads.append(obj)
    for thread in threads:
        thread.join()
    obj.dadosSelect()
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