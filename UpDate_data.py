import csv
import pandas as pd
from pandas_datareader import data as wb
import threading
import os.path
import yfinance as yf
from datetime import date, timedelta
import monthdelta
from time import perf_counter
from time import sleep
import MetaTrader5 as mt5

class Atualizar(threading.Thread):

    def __init__(self, tipo, descartados, mutex, intervalo):
        self.tipo = tipo
        self.descartados = descartados
        self.mutex = mutex
        self.intervalo = intervalo
        threading.Thread.__init__(self)

    def comparar_dados(self):

        # COLETANDO OS DADOS DO ARQUIVO YAHOO
        #papel = self.listaAcao[0]
        #papel = 'MGLU3'
        #self.listaAcao = ['AALR3']
        # Utilizando os nomes dos arquivos já baixados
        ##lista = os.listdir(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_yahoo_CSV')
        ##self.listaAcao = [x.replace('.csv', '') for x in lista]
        # Utilizando somente a lista das opções com liquidez
        #self.listaAcao = ['PETR4.SA']
        self.listaAcao = ['BOVA11.SA', 'ABEV3.SA', 'BBDC4.SA', 'CIEL3.SA', 'CSNA3.SA', 'GGBR4.SA',
                          'ITUB4.SA', 'ITSA4.SA', 'PETR4.SA', 'USIM5.SA', 'VALE3.SA',
                          'BRFS3.SA', 'SBSP3.SA', 'EMBR3.SA', 'SUZB3.SA', 'MRFG3.SA', 'JBSS3.SA', 'WEGE3.SA',
                          'MGLU3.SA', 'VIIA3.SA', 'COGN3.SA', 'BBAS3.SA',
                          'B3SA3.SA', 'BPAC11.SA', 'BBSE3.SA', 'CMIG4.SA']
        with self.mutex:

            for papel in self.listaAcao:
                print(papel)
                # Verificando se o arquivo existe
                if os.path.exists(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_yahoo_CSV/{self.intervalo}/{papel}.csv'):
                    dadosY = pd.read_csv(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_yahoo_CSV/{self.intervalo}/{papel}.csv', sep=',', engine='python')
                    # dadosAcao = pd.read_csv(f'F:/OneDrive/Cursos Python/AntiFragil/Blue Chips/{self.papel}.csv', sep=',', engine='python')  # ECOR3.SA
                    # Removendo os dados nulos (NaN)
                    #sleep(0.05)
                    try:
                        dadosY.dropna(inplace=True)
                        # Removendo os dados nulos
                        dadosY.dropna(inplace=True)
                        # Removendo por filtro valores igual a zero
                        self.dadosY_CSV = dadosY[dadosY.iloc[0:, 1] > 0]
                    except ValueError:
                        pass
                    pass
                    # Convertendo a Data para datetime
                    #self.dadosY_CSV['Date'] = self.dadosY_CSV['Date'].astype('datetime64[ns]')

                    #COLETANDO OS DADOS DO ARQUIVO DA YAHOO
                    # Coletando o período de 1 mês
                    # Obtendo a data de hoje
                    #hoje = date.today()

                    # Subtraindo os meses para obter a data inicial
                    #data_i = hoje - monthdelta.monthdelta(1)

                    try:
                        # conecte-se ao MetaTrader 5
                        if(self.tipo=="MT5"):
                            if not mt5.initialize():
                                print("initialize() failed, error code =", mt5.last_error())
                                quit()

                            # consultamos o estado e os parâmetros de conexão
                            #print(mt5.terminal_info())
                            # obtemos informações sobre a versão do MetaTrader 5
                            print(mt5.version())
                            #yf.pdr_override()
                            #dados = wb.DataReader(f'{papel}', data_source='yahoo', start=f'{data_i}', end=f'{hoje}')
                            # solicitamos n barras do papel
                            if(self.intervalo == '1d'):
                                dados = mt5.copy_rates_from_pos(f"{papel.replace('.SA','')}", mt5.TIMEFRAME_D1, 0, 100)
                            elif(self.intervalo == '1wk'):
                                dados = mt5.copy_rates_from_pos(f"{papel.replace('.SA','')}", mt5.TIMEFRAME_W1, 0, 30)
                            # concluímos a conexão ao terminal MetaTrader 5
                            mt5.shutdown()
                            # exibimos cada elemento de dados recebidos numa nova linha
                            '''print("Exibimos os dados recebidos como estão")
                            for rate in dados:
                                print(rate)'''
                            # a partir dos dados recebidos criamos o DataFrame
                            rates_frame = pd.DataFrame(dados)
                            # convertemos o tempo em segundos no formato datetime
                            rates_frame['time'] = pd.to_datetime(rates_frame['time'], unit='s')
                            # exibimos dados
                            #print("\nExibimos o dataframe com dados")
                            #print(rates_frame)
                        else:
                            # Usando o Yahoo
                            yf.pdr_override()
                            rates_frame = yf.download(tickers=f'{papel}', period='1mo', group_by='ticker', interval=self.intervalo)
                            sleep(0.05)
                    except (SystemError, TypeError):
                        pass

                    # Condição para verificar se a última data é igual a de hoje, se sim deletar do banco de dados e atualizar somente esta
                    #Obtendo a data de hoje
                    hoje = date.today()
                    #hoje = date.today() - timedelta(1)
                    if(self.tipo=="MT5"):
                        dado_hoje = rates_frame.iloc[-1, 1] != 0
                    else:
                        dado_hoje = rates_frame.iloc[-1, 0] != 0
                    # Condição para verificar se a última data do banco de dados é diferente de hoje e os dados são diferente de ZERO tanto no BD quanto no dia atual
                    if (self.dadosY_CSV.iloc[-1, 0] != str(hoje)) and (self.dadosY_CSV.iloc[-1, 1] != 0) and (dado_hoje):
                        # Coletando as datas e convertendo para String (Metatrader)
                        if(self.tipo == "MT5"):
                            dias = [date.strftime(x, '%Y-%m-%d') for x in rates_frame.iloc[0:, 0]]
                        # Coletando as datas e convertendo para String (Yahoo)
                        else:
                            dias = list(rates_frame.Open.index.astype(str))
                        self.count = 0
                        # Verificando as datas diferentes
                        for x in dias[::-1]:
                            if x != self.dadosY_CSV.iloc[-1, 0]:
                                self.count += 1
                            elif x == self.dadosY_CSV.iloc[-1, 0]:
                                break
                            else:
                                pass
                        print(self.count)

                        with open(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_yahoo_CSV/{self.intervalo}/{papel}.csv', 'a', newline='') as arquivo:
                            writer = csv.writer(arquivo)
                            for x in range(0, self.count):
                                # 'Date', 'Open', 'High', 'Low', 'Close', 'Variation', 'Volume'
                                #Metatrader
                                if(self.tipo=="MT5"):
                                    writer.writerow((dias[-(self.count-x)], rates_frame.iloc[-(self.count-x), 1], rates_frame.iloc[-(self.count-x), 2], rates_frame.iloc[-(self.count-x), 3],
                                                rates_frame.iloc[-(self.count-x), 4], ((rates_frame.iloc[-(self.count-x), 3] / rates_frame.iloc[-(self.count-x), 2]) - 1) * 100, rates_frame.iloc[-(self.count-x), 7]))
                                # Yahoo
                                else:
                                    writer.writerow((dias[-(self.count - x)], rates_frame.iloc[-(self.count - x), 0],
                                                 rates_frame.iloc[-(self.count - x), 1], rates_frame.iloc[-(self.count - x), 2],
                                                 rates_frame.iloc[-(self.count - x), 3], ((rates_frame.iloc[-(self.count - x), 2] / rates_frame.iloc[-(self.count - x), 1]) - 1) * 100,
                                                 rates_frame.iloc[-(self.count - x), 5]))
                    # Condição para atualizar o dado do mesmo dia
                    elif(rates_frame.iloc[-1, 0] != 0):
                        #Copiando os dados do arquivo e deletando
                        with open(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_yahoo_CSV/{self.intervalo}/{papel}.csv', 'r', newline='') as arquivo:
                            read = list(csv.reader(arquivo))
                            # Deletando a última linha da lista
                            read.pop()
                        if os.path.exists(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_yahoo_CSV/{self.intervalo}/{papel}.csv'):
                            os.remove(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_yahoo_CSV/{self.intervalo}/{papel}.csv')
                        '''
                        # Caso ocorra problema de valor inválido na última atualização e não fizer o download antes do horário do pregão do dia seguinte
                        with open(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_yahoo_CSV/{papel}.csv', 'w',
                                  newline='') as arquivo:
                            writer = csv.writer(arquivo)
                            for x in read:
                                # 'Date', 'Open', 'High', 'Low', 'Close', 'Variation', 'Volume'
                                writer.writerow(x)

                        '''
                        # Reescrevendo e acrescentando a atualização
                        with open(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_yahoo_CSV/{self.intervalo}/{papel}.csv', 'w', newline='') as arquivo:
                            writer = csv.writer(arquivo)
                            for x in read:
                                # 'Date', 'Open', 'High', 'Low', 'Close', 'Variation', 'Volume'
                                writer.writerow(x)
                            # 'Date', 'Open', 'High', 'Low', 'Close', 'Variation', 'Volume'
                            # Coletando as datas e convertendo para String (Metatrader)
                            if(self.tipo=="MT5"):
                                dias = [date.strftime(x, '%Y-%m-%d') for x in rates_frame.iloc[0:, 0]]
                            # Coletando as datas e convertendo para String (Yahoo)
                            else:
                                dias = list(rates_frame.Open.index.astype(str))
                            # Escrevendo dados obtidos do Metatrader
                            if(self.tipo=="MT5"):
                                writer.writerow((dias[-1], rates_frame.iloc[-1, 1],
                                             rates_frame.iloc[-1, 2], rates_frame.iloc[-1, 3],
                                             rates_frame.iloc[-1, 4], ((rates_frame.iloc[-1, 3] / rates_frame.iloc[-1, 2]) - 1) * 100,
                                             rates_frame.iloc[-1, 7]))
                            # Escrevendo dados obtidos do Yahoo
                            else:
                                writer.writerow((dias[-1], rates_frame.iloc[-1, 0],
                                             rates_frame.iloc[-1, 1], rates_frame.iloc[-1, 2],
                                             rates_frame.iloc[-1, 3],
                                             ((rates_frame.iloc[-1, 2] / rates_frame.iloc[-1, 1]) - 1) * 100,
                                             rates_frame.iloc[-1, 5]))
                else:
                    pass

if __name__ == '__main__':
    descartados = ''
    stdoutmutex = threading.Lock()
    # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
    # (optional, default is '1d')
    interval = "1wk"
    threads = []
    tipo = "MT5"
    obj = Atualizar(tipo, descartados, stdoutmutex, interval)
    threadCOMP = threading.Thread(target=obj.comparar_dados())
    threadCOMP.daemon = True
    threadCOMP.start()
    threads.append(threadCOMP)
    for thread in threads:
        thread.join()
    duracao = round((perf_counter()), 0)
    horas = int(duracao // 3600)
    minutos = int(round((duracao / 3600 - duracao // 3600) * 60, 0))
    segundos = int(round((duracao % 60), 0))
    print(f'Tempo de execução:{horas}:{minutos}:{segundos}')