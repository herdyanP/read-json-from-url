import datetime
import json
import time

import requests
import schedule
import yaml

import mysql.connector
import logging

logdate = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
logging.basicConfig(filename=f'{logdate}.log', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

statSuccess = True
data = None

def init():
    global statSuccess
    statSuccess = False
    start()

def start():
    global statSuccess
    now = datetime.datetime.now()
    wk = now.isoweekday()

    if(wk <= 5): # biar cuma jalan di weekday
        # Connect ke database
        printWithStamp('Connecting to database', end = '...', flush=True)
        try:
            mydb = mysql.connector.connect(
                host=config['dbHost'],
                user=config['dbUser'],
                password=config['dbPass'],
                database=config['dbName']
            )
            print('Done')
        except Exception as e:
            print('Fail')
            logging.exception('Exception occured when connecting to database!')
            quit()

        # Fetch data dari API
        dtformat = now.strftime('%Y%m%d')
        printWithStamp(f'Fetching JSON {dtformat}', end='...', flush=True)
        try:
            response = json.loads(requests.get(f"http://{config['apiUrl']}:{config['apiPort']}/webservice/mcollection/InqData?token={config['apiToken']}&x=1&y=90000").text)
            print(f'{response["msg"]}')
        except:
            print('Fail')
            logging.exception('Exception occured when fetching JSON!')
            quit()

        # Insert ke database
        printWithStamp(f'Processing data', end = '...', flush=True)
        
        try:
            mycur = mydb.cursor()

            line_count = 0
            now = datetime.datetime.now()
            dtnow = now.strftime('%Y-%m-%d')

            sql = f'TRUNCATE TABLE saldotabungan_tmp'
            mycur.execute(sql)

            for data in response['data']:
                namaclear = data['NAMA'].replace("'", "")
                if data['ALAMAT'] is not None:
                    alamclear = data['ALAMAT'].replace("'", "")

                sql = f'INSERT INTO saldotabungan_tmp (CIF, SSREK, SSNAMA, SSALAMAT, SSSALDO, SSTGL, JPINJAMAN, TGLDATA) VALUES ("{data["CIF"]}", "{data["NO_REKENING"]}", "{namaclear}", "{alamclear}", "{data["SALDO"]}", "{dtnow}", "{data["JENIS_REKENING"]}", "{data["TGL_DATA"]}");'
                mycur.execute(sql)

                line_count += 1

            # Pindah data non-duplikat dari saldotabungan_tmp ke saldotabungan
            sql = f'INSERT INTO saldotabungan (CIF, SSREK, SSNAMA, SSALAMAT, SSSALDO, SSTGL, SSUSERID, SSTGLSTAMP, JPINJAMAN, `STATUS`, TGLDATA) SELECT CIF, SSREK, SSNAMA, SSALAMAT, SSSALDO, SSTGL, SSUSERID, SSTGLSTAMP, JPINJAMAN, `STATUS`, TGLDATA FROM saldotabungan_tmp WHERE SSREK NOT IN (SELECT SSREK FROM saldotabungan WHERE SSTGL = "{dtnow}");'
            mycur.execute(sql)
            
            print('Done')
            printWithStamp(f'Processed {line_count} lines')

            mydb.commit()
            mycur.close()
        except Exception as e:
            print('Fail')
            logging.exception('Exception occured when inserting to database!')
            quit()

def printWithStamp(*args, **kwargs):
    dt = datetime.datetime.now()
    ts = dt.strftime('%Y-%m-%d %H:%M:%S')
    print(f'({ts}) ' + ' '.join(map(str,args)), **kwargs)


try:
    config = yaml.safe_load(open('config.yml'))
except Exception as e:
    logging.exception('Exception occured')
    printWithStamp(f'Fetching JSON', end='...', flush=True)

    time.sleep(10)
    quit()

# ========== MAIN CODE HERE ===========
print("====== UPLOADER MCOLLECTION JOGJA ======")
# schedule.every().day.at("04:30").do(init)
# while True:
    # schedule.run_pending()
    # time.sleep(1)

init()

# buat compile:
# pyinstaller --onefile --clean [nama_file]

# ==========================================================================

# def cobacoba():
#     config = yaml.safe_load(open('config.yml'))
#     userAgent = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'}
#     response = json.loads(requests.get(f"http://{config['apiUrl']}:{config['apiPort']}/webservice/mcollection/InqData?token={config['apiToken']}&x=1&y=10&tgl_data=20221114", headers=userAgent).text)
#     for data in response['data']:
#         print(data)
#         # dtnow = datetime.strptime(data['TGL_DATA'], '%Y%m%d')
#         # print(dtnow.strftime('%d-%m-%Y'))

# # print(cobacoba())
# cobacoba()