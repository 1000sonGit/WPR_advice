import yfinance as yf
from datetime import date
import pandas as pd
import csv
import threading
import os.path
from time import perf_counter
#from pandas_datareader import data as pdr
from time import sleep

class Coleta(threading.Thread):

    def __init__(self, mutex, descartados):
        self.mutex = mutex
        self.descartados = descartados
        threading.Thread.__init__(self)

    def getting(self):

        #Utilizando os nomes dos arquivos já baixados
        #lista = os.listdir(f'F:/OneDrive/Cursos Python/Gritos do Mercado/Dados_yahoo_CSV')
        #self.listaAcao = [x.replace('.csv', '') for x in lista]
        self.listaAcao = ['BBAS3.SA']
        '''self.listaAcao = ['BOVA11.SA', 'ABEV3.SA', 'BBDC4.SA', 'CIEL3.SA', 'CSNA3.SA', 'GGBR4.SA',
                          'ITUB4.SA', 'ITSA4.SA', 'PETR4.SA', 'USIM5.SA', 'VALE3.SA',
                          'BRFS3.SA', 'SBSP3.SA', 'EMBR3.SA', 'SUZB3.SA', 'MRFG3.SA', 'JBSS3.SA', 'WEGE3.SA',
                          'MGLU3.SA', 'VIIA3.SA', 'COGN3.SA', 'BBAS3.SA',
                          'B3SA3.SA', 'BPAC11.SA', 'BBSE3.SA', 'CMIG4.SA']'''

        print(self.listaAcao)
        with self.mutex:
            for name in self.listaAcao:
                try:
                    yf.pdr_override()
                    dados = yf.download(tickers=f'{name}', period='max', group_by='ticker')
                    #dados = pdr.get_data_yahoo(f"{name}", period='max')
                    sleep(0.05)
                    #print(dados)
                    if len(dados.iloc[0:, 0]) > 0:
                        if os.path.exists(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_yahoo_CSV/{name}.csv'):
                            os.remove(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_yahoo_CSV/{name}.csv')
                        with open(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_yahoo_CSV/{name}.csv', 'w', newline='') as arquivo:
                            writer = csv.writer(arquivo)
                            # Inserindo cabeçalho
                            writer.writerow(('Date', 'Open', 'High', 'Low', 'Close', 'Variation', 'Volume'))
                            date = list(dados.Open.index.astype(str))
                            for i in range(0, len(dados.Open)):
                                writer.writerow((date[i], dados.iloc[i, 0], dados.iloc[i, 1], dados.iloc[i, 2], dados.iloc[i, 3], ((dados.iloc[i, 3] / dados.iloc[i, 0]) - 1) * 100, dados.iloc[i, 5]))
                    else:
                        pass
                except SystemError:
                    pass


if __name__ == '__main__':
    stdoutmutex = threading.Lock()
    descartados = ''
    threads = []
    obj = Coleta(stdoutmutex, descartados)
    threadGET = threading.Thread(target=obj.getting())
    threadGET.daemon = True
    threadGET.start()
    threads.append(threadGET)
    for thread in threads:
        thread.join()
    duracao = round((perf_counter()), 0)
    horas = int(duracao // 3600)
    minutos = int(round((duracao / 3600 - duracao // 3600) * 60, 0))
    segundos = int(round((duracao % 60), 0))
    print(f'Tempo de execução:{horas}:{minutos}:{segundos}')