"""
Created on Sun Feb  6 08:19:42 2022

@author: JeronimoH
"""

from datetime import datetime, date

import os
import json
import requests
import pandas as pd
import hmac, hashlib
from dotenv import load_dotenv
from binance.client import Client
from datetime import datetime, timezone, timedelta

"""
-------------------------------------------------------------------------------
Toda info del més pasado
-------------------------------------------------------------------------------
"""
##########################################################################################################
NAV0 = 105

# cartera0 => Valuación de la cartera del més anterior
cartera0 = 21050#1801
print(f'El valor de la cartera a principio de més: ${cartera0}\n')

#Define 2 maturities 
trimestral = "220325"
semestral = "220624"
##########################################################################################################
"""LAS DE CHAKANA (JERO) """
api_key = ""
api_secret = ""
"""
Requesting API info from environment file
"""
load_dotenv()

os.chdir('C:/Users/Usuario/Desktop/Chakana/SpecialOps/binance/Contailidad')

#api_key = os.getenv('API_KEY')
#api_secret = os.getenv("API_SECRET")


"""
Passing intel from Binance official API docs
"""

client = Client(api_key,api_secret)

fut_url = "https://dapi.binance.com/dapi/v1/positionRisk"#userTrades

spot_url = "https://api.binance.com/api/v3/myTrades"



#Make a for loop for all traded tikkers: 
#my_tickers = ["XRPUSD_220624", "LINKUSD_220624"]


#For loop: 
#Defining UTC timestamp of NOW for request
now = datetime.now(timezone.utc)
epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)  # use POSIX epoch
posix_timestamp_micros = (now - epoch) // timedelta(microseconds=1)
posix_timestamp_millis = posix_timestamp_micros // 1000  # or `/ 1e3` for float

#Creating request query
queryString = "timestamp=" + str(
    posix_timestamp_millis)
signature = hmac.new(api_secret.encode(), queryString.encode(), hashlib.sha256).hexdigest()

url = fut_url + f"?{queryString}&signature={signature}"
response = requests.get(url, headers={'X-MBX-APIKEY': api_key})

#Response
#print(response.json())
future_positions = pd.DataFrame(json.loads(response.text))

future_positions['date'] = pd.to_datetime(future_positions['updateTime'], unit='ms')
#Order by time
future_positions = future_positions.sort_values(by="date")



#Reset index
future_positions = future_positions.reset_index(drop=True)
complete_df_f = pd.DataFrame(columns = future_positions.columns)



#Make a for loop for all traded tikkers: 
##########################################################################################################
my_tickers = ["XRPUSDT", "LINKUSDT", "LTCUSDT"]

##########################################################################################################

complete_list_s = []

no_fut = True
for i in range(0,len(future_positions)):
    #print(i)
    #print(future_positions["positionAmt"][i])
    if str(future_positions["positionAmt"][i]) != "0":
        #print(future_positions.loc[i])        
        complete_df_f=complete_df_f.append(future_positions.loc[i])
        no_fut=False

    else:
        no_fut=True

if no_fut == True:
    print("No future positions open !\n RUN THE USDT_IN_SPOT CALL\n That will be the NAV1... compare it to NAV0")
else:

    
    
    
    #For loop: 
    for i in range(0,len(my_tickers)):
        
        #Defining UTC timestamp of NOW for request
        now = datetime.now(timezone.utc)
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)  # use POSIX epoch
        posix_timestamp_micros = (now - epoch) // timedelta(microseconds=1)
        posix_timestamp_millis = posix_timestamp_micros // 1000  # or `/ 1e3` for float
        
        #Creating request query w/ tikker, timestamp & signature
        queryString = "symbol=" + my_tickers[i] + "&timestamp=" + str(
            posix_timestamp_millis)
        signature = hmac.new(api_secret.encode(), queryString.encode(), hashlib.sha256).hexdigest()
        #url = "https://api.binance.com/api/v3/myTrades"
        url = spot_url + f"?{queryString}&signature={signature}"
        response = requests.get(url, headers={'X-MBX-APIKEY': api_key})
        
        #Response
        #print(response.json())
        response_df = pd.DataFrame(json.loads(response.text))
        #print(response_df.columns)
        
        #add every df got into a list
        complete_list_s.append(response_df)
    
    
    complete_df_s = pd.DataFrame()
    
    for df in complete_list_s: 
        complete_df_s = complete_df_s.append(df)
    
    
    
    #Unix to UTC time conversion
    complete_df_s['date'] = pd.to_datetime(complete_df_s['time'], unit='ms')
    
    #Order by time
    complete_df_s = complete_df_s.sort_values(by="date")
    
    
    #Reset index
    complete_df_s = complete_df_s.reset_index(drop=True)
    
    
    
    
    #Check spot price matches
    
    
    #make array for avrging spot prices
    prices_spot = []
    timestamps_spot =[]
    amounts_spot = []
    
    maturity = []
    mat_type = []
    
    
    pos_found = {}
    counter = 0

    for j, row in complete_df_s.iterrows():
        ### THIS IS ALL THE SPOT DATA ###
        print('index: ', j, 'symbol:', row['symbol'])
    
        spot_tik = row["symbol"]
        spot_pricex = float(row['price'])
        spot_amountx = float(row["quoteQty"])
        spot_orderidx = row["orderId"]
        spot_timexr = pd.to_datetime(row["time"], unit='ms')
        spot_timex = row["time"]
        spot_dy = spot_timexr.day
        print(spot_dy)
    
        
        for i in complete_df_f.loc[:,'symbol']:
            fut_tik = i[:-7]
            full_tik = i
            for i in complete_df_f.loc[:,'updateTime']:
                #print(pd.to_datetime(i, unit='ms'))
                fut_dy = pd.to_datetime(i, unit='ms').day
    
                if fut_tik in spot_tik and spot_dy == fut_dy:
                    print("found! ---")
                    counter +=1
                    #print(spot_pricex)
                    #print(spot_amountx)
                    #print(spot_timexr)
                    
                    pos_found[f'{spot_tik} #{counter}'] = {
                                    "precio": spot_pricex,
                                    "monto": spot_amountx,
                                    "tiemp": spot_timexr,
                                    "timestmp":spot_timex,
                                    "contrato": full_tik, 
                                    "orderId": spot_orderidx
                                    }
                    
    df = df.from_dict(pos_found).transpose().reset_index()
    df.rename({'index': 'symbol'}, axis=1, inplace=True)
    
    names = pd.Series(df["symbol"])
    names = names.str[:-3]
    df["symbol"] = names
    
    
    single_orders = df.drop_duplicates(subset=['orderId', 'tiemp'], keep=False).reset_index(drop=True)
    many_orders = df[df.duplicated(subset=["orderId", "symbol"], keep=False)].reset_index(drop=True)
        
        
    """
        REGISTER ALL INFO ON SINGLE ORDERS
    """

    for i in range(len(single_orders)):
        #print(i)
        #print(single_orders.loc[i, "precio"], single_orders.loc[i, "symbol"])
        #Add average spot price and time to list        
        prices_spot.append(single_orders.loc[i, "precio"])
        timestamps_spot.append(float(single_orders.loc[i, "timestmp"]))
        amounts_spot.append(single_orders.loc[i, "monto"])
        
        
        short_date = single_orders.loc[i, "contrato"][single_orders.loc[i, "contrato"].find('_')+1:]
        #print(short_date)
        #maturity.append(f'20{short_date[:-4]}-{short_date[4:]}-{short_date[2:-2]} 00:00:00')
        maturity.append(date(int(f'20{short_date[:-4]}'),int(f'{short_date[2:-2]}'),int(f'{short_date[4:]}')))
        #print(maturity)
        if single_orders.loc[i, "contrato"][-6:] == semestral:
            mat_type.append("semestral")
            #print(mat_type)
                
    """
        REGISTER ALL INFO ON MULTIPLE ORDERS
    """
    
    cantzxx = many_orders.groupby(["symbol"])["monto"].sum()
    pricezxx = many_orders.groupby(["symbol"])["precio"].mean()
    timezxx = many_orders.groupby(["symbol"])["timestmp"].mean()
    timestampzxx = many_orders.groupby(["symbol"])["tiemp"].mean()
    contractzxx = many_orders.groupby(["symbol"])["contrato"].apply(lambda tags: '/'.join(tags))

    
    for i in range(len(cantzxx)):
        #print(i)
        
        #Add average spot price and time to list        
        prices_spot.append(pricezxx[i])
        timestamps_spot.append(timezxx[i])
        amounts_spot.append(cantzxx[i])
        

        short_date = contractzxx[i].split("/")[0]
        short_date = short_date.split("_")[1]
        #print(short_date)
        #maturity.append(f'20{short_date[:-4]}-{short_date[4:]}-{short_date[2:-2]} 00:00:00')
        maturity.append(date(int(f'20{short_date[:-4]}'),int(f'{short_date[2:-2]}'),int(f'{short_date[4:]}')))
        #print(maturity)
        if many_orders.loc[i, "contrato"][-6:] == semestral:
            mat_type.append("semestral")
            #print(mat_type)
        
        
        
    
    tasas_dir = []
    tasas_anuales = []
    
        
    for i in range(0, len(complete_df_f)):
        
        spot = float(prices_spot[i])
        fut = float(complete_df_f.iloc[i]['entryPrice'])

        print('F = $' + str(round(fut, 1)))
        print("-----------")
        print('S = $' + str(spot))
        

        tasas_dir.append((fut/spot)-1)
    
    
    """ Make VENC timestamp automatic !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1"""
    for i in range(0,len(tasas_dir)):
        #print(i)
        #print(timestamps_spot[i])
        #print(len(str(timestamps_spot[i])[:-2]))#should be 13
        spot_date = datetime.fromtimestamp(int(float(str(timestamps_spot[i])[:-2]))/1000)
        print(spot_date)
        ##########################################################################################################
        #Agregar venc = venc nashi
        venc = datetime.fromtimestamp(1656068400000/1000)
        
        ##########################################################################################################

        
        dif = venc - spot_date
        days = dif.days
              
        tasas_anuales.append(((((tasas_dir[i]/days)+1)**365-1)))
            
    
    print(f'\nTasas anuales = {tasas_anuales}\n')
    
    
            
    
    """
    -------------------------------------------------------------------------------
    Buscar el portafolio actual
    -------------------------------------------------------------------------------
    """
    
    account_url = "https://api.binance.com/api/v3/account"
    
    usdt_in_spot = 0
    
    #Defining UTC timestamp of NOW for request
    now = datetime.now(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)  # use POSIX epoch
    posix_timestamp_micros = (now - epoch) // timedelta(microseconds=1)
    posix_timestamp_millis = posix_timestamp_micros // 1000  # or `/ 1e3` for float
    
    #Creating request query w/ tikker, timestamp & signature
    queryString = "timestamp=" + str(posix_timestamp_millis)
    signature = hmac.new(api_secret.encode(), queryString.encode(), hashlib.sha256).hexdigest()
    url = account_url + f"?{queryString}&signature={signature}"
    response = requests.get(url, headers={'X-MBX-APIKEY': api_key})
    
    account_intel = json.loads(response.text)
    
    for i in account_intel["balances"]:
        if i["asset"] == "USDT":
            usdt_in_spot = int(float(i["free"]))
    
    
    #print(f'THEUSDT IN SPOT:{usdt_in_spot}')
    
    ###CREATE THE NEW "COLOCACIONES" DICTIONARY###
    colocaciones = {}
    
    for i in range(0,len(tasas_dir)):
        #print(i)
        
        ##cartera##
        colocaciones.update({
            
            f'coloc_#{i+1}':{
                        "monto": amounts_spot[i],
                        "tasa_dir": tasas_dir[i],
                        "ponderacion" : amounts_spot[i]/usdt_in_spot,
                        #"fechacolocada": datetime.strptime('2022 01 05', '%Y %m %d').date(),
                        "tipo_madurez": mat_type[i],
                        "vencimiento": maturity[i]
            },
        })
        
        
    print(f'Colocaciones: {colocaciones}\n')
         
        
    
    
    """
    -------------------------------------------------------------------------------
    Toda info de este més
    -------------------------------------------------------------------------------
    """
    
    NAV1 = None
    
    fila_entrada = {
        
        "inversor1":{
            "Nombre":"Ronald Mc D.",
            "Monto": 500,
            },
        
        "inversor2":{
            "Nombre":"Juan Román",
            "Monto": 50,
            },
        
        "inversor3":{
            "Nombre":"Elvis Prestobarba",
            "Monto": 5,
            }
        
        }
    
    
    
    fila_salida = {
        
        "inversor1":{
            "Nombre":"Christian Bale",
            "Capital": 100,
            "nav_entrada": 100
            },
        
        "inversor2":{
            "Nombre":"Jeff Besos",
            "Capital": 10,
            "nav_entrada": 101
            },
        
        "inversor3":{
            "Nombre":"Tu bisabuela",
            "Capital": 1,
            "nav_entrada": 102
            }
        
        }
    
    
    entradas_total = 0
    
    for item in fila_entrada.values():
        #print(item["Monto"])
        entradas_total += item["Monto"]
        
        
    salidas_total = 0
    
    for item in fila_salida.values():
        #print(item["Monto"])
        salidas_total += item["Capital"]
        
    
    """ Ahora sí a valuar la cartera """
         
    
    cartera1 = 0
    
    for item in colocaciones.values():
    
        """
        total_time = item["vencimiento"] - item["fechacolocada"]
        total_time = pd.to_timedelta([total_time]).astype('timedelta64[D]')[0]
        #print(total_time)
        
        time_delta = item["vencimiento"] -  date.today()
        time_delta = pd.to_timedelta([time_delta]).astype('timedelta64[D]')[0]
        #print(time_delta)
        """
        
        month_delta = item["vencimiento"].month - date.today().month
        #print(month_delta)
        #Sacar el factor de descuento
        if item["tipo_madurez"] == "trimestral":
                
            if month_delta == 1: 
                factor_desc = 1
            if month_delta == 2: 
                factor_desc = 2/3
            if month_delta == 3: 
                factor_desc = 1/3
            
    
        if item["tipo_madurez"] == "semestral":
    
            if month_delta == 1:
                factor_desc = 1
            if month_delta == 2: 
                factor_desc = 5/6
            if month_delta == 3: 
                factor_desc = 4/6
            if month_delta == 4: 
                factor_desc = 3/6
            if month_delta == 5: 
                factor_desc = 2/6
            if month_delta == 6: 
                factor_desc = 1/6
        #print(factor_desc)
        #print(f'TASA DIR {item["tasa_dir"]}')
        #print(f'USDT IN SPOT {usdt_in_spot}')
    
    
        VA_tasa =  item["monto"] * (1+ item["tasa_dir"]*factor_desc)   #THERE WAS AN ERROR HERE
        #print(VA)
        cartera1 += VA_tasa
    
    #print(cartera1)
    
    cartera_final = cartera1 + usdt_in_spot

    #print(f'CARTERA 1: {cartera1}')
    #print(f'CARTERA FINAL: {cartera_final}')
    
    """ ACÁ PODÉS VER QUE LAS POSICIONES EN CASH SE TOMAN EN CUENTA SIN ACTUALIZAR"""
    delta_indice = ( cartera_final / (cartera0))-1
    #print(delta_indice)
    
    
    
    NAV1 = NAV0*(1+delta_indice)
    print(f'\nEl NAV viejo: ${NAV0}, creció este més a: {round(NAV1,3)}')
    
    
    print(f'\nEl valor de la cartera final: {round(cartera_final, 3)}')

    
    """
    Como quedan calzadas las filas de salida 
    """
    
    for item in fila_salida.values():
        
        item["nav_salida"] = NAV1
        item["val_salida"] = item["Capital"]* (1+ ( NAV1/item["nav_entrada"] )-1)  #ACÁ ES NAV_deentrada de cada entrador!!!!!!!!!
    
    """
    Como queda la cartera
    """
    
    
    #result should be
    #print(cartera_final - 5020 - 502 - 50.2 + 1000 + 100 + 10)
    
    for item in fila_salida.values():
        cartera_final = cartera_final - item["val_salida"]
        
    for item in fila_entrada.values():
        cartera_final = cartera_final + item["Monto"]
        
        
    
    print("\n------------- Fila Salida Queda así: ")
    
    print(fila_salida)
    
    print("\n------------- Fila Entrada Queda así: ")
    
    #AGREGAR NAV ENTRADA
    
    print(fila_entrada)
    
    print("\n------------- LA CARTERA INICIAL PARA EL MÉS QUE VIENE ES: ")
    
    print(round(cartera_final,2))
        
    
