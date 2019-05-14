import requests
import pandas as pd
import numpy as np
import os
import re
import logging


def get_current_pollution(stations_csv, save_path, max_history=6):
    stations_df = pd.read_csv(stations_csv)
    stations_df = stations_df[stations_df['DOSTEPNOSC'] == True]
    new_data = {'STACJA': [], 'DATA': [], 'POMIAR': [], 'WARTOSC': []}
    for sid in stations_df['ID']:
        url = f'http://api.gios.gov.pl/pjp-api/rest/station/sensors/{sid}'
        response = requests.get(url).json()
        for sensor in response:
            measure_name = sensor['param']['paramCode']
            if not measure_name in ['PM10', 'PM2.5', 'PM25']:
                continue
            if measure_name == 'PM25':
                measure_name = 'PM2.5'
            sensor_id = sensor['id']
            sensor_url = f'http://api.gios.gov.pl/pjp-api/rest/data/getData/{sensor_id}'
            sensor_response = requests.get(sensor_url).json()
            for i in range(min(max_history, len(sensor_response['values']))):
                if sensor_response['values'][i]['value'] != None:
                    measure_value = sensor_response['values'][i]['value']
                    measure_date = sensor_response['values'][i]['date']
                    new_data['STACJA'].append(sid)
                    new_data['DATA'].append(measure_date)
                    new_data['WARTOSC'].append(measure_value)
                    new_data['POMIAR'].append(measure_name)
                    break
    df = pd.DataFrame.from_dict(new_data)
    df.to_csv(save_path, index=False)


def resave(excel_path, csv_path, stations_csv):
    stations_df = pd.read_csv(stations_csv)
    df = pd.read_excel(excel_path, header=None)
    first_col = df.columns[0]
    header = df[df[first_col].str.lower() == 'kod stacji'].values.flatten()
    if len(header) == 0:
        for i, row in df.iterrows():
            if row.str.contains('1g').any():
                header = df.iloc[i-2]
                break
        else:
            logging.warning(f'Cannot parse {excel_path}')
            return
    
    header[0] = 'Date'
    df = df[~pd.to_datetime(df[first_col], errors='coerce').isna()]
    df.columns = header

    rename_mapping, unknown = {}, []
    for col in df.columns[1:]:
        val = stations_df.loc[stations_df['KOD NOWY'] == col, 'ID']
        if not val.empty:
            rename_mapping[col] = val.item()
        else:
            unknown.append(col)
    df = df.drop(unknown, axis=1)
    df = df.rename(columns=rename_mapping)

    df.to_csv(csv_path, index=False)
    
    
def excel_to_csv(dir_path, stations_csv, year, measure):
    m_name, possible_m_names = measure
    for m in possible_m_names:
        excel_name = f'{year}_{m}_1g.xlsx'
        excel_path = os.path.join(dir_path, excel_name)
        if not os.path.exists(excel_path):
            continue
            
        csv_name = f'{m_name}.csv'
        csv_path = os.path.join(dir_path, csv_name)
        resave(excel_path, csv_path, stations_csv)
        return
    logging.warning(f'{m_name} not found in {year}')
    
    
def handle_zip(data_dir, fname):
    old_fname = fname
    if '.zip' not in fname.lower():
        return '', -1
            
    fname = re.sub(r'(.zip)+', ".zip", fname.lower())
    year = fname[:-4]
    if not year.isdecimal():
        return '', -1

    zip_path = os.path.join(data_dir, fname)
    if fname != old_fname:
        mv_command = f'mv {os.path.join(data_dir, old_fname)} {zip_path}'
        os.system(mv_command)
    new_dir_path = os.path.join(data_dir, year)
    os.system(f"mkdir {new_dir_path}; unzip {zip_path} -d {new_dir_path}; rm {zip_path};")
    return new_dir_path, year
    
    
def remove_excels(dir_path):
    excels = [os.path.join(dir_path, fname) for fname in os.listdir(dir_path) if '.xlsx' in fname]
    rm_command = 'rm \'' + '\' \''.join(excels) + '\''
    os.system(rm_command)

    
def load_data(data_dir, stations_csv, measures):
    for fname in os.listdir(data_dir):
        new_dir_path, year = handle_zip(data_dir, fname)
        if not new_dir_path:
            continue
            
        for m in measures.items():
            excel_to_csv(new_dir_path, stations_csv, year, m)

        remove_excels(new_dir_path)


if __name__ == '__main__':
    smog_dir_path = os.path.join('..', 'data', 'smog')
    stations_csv = os.path.join(smog_dir_path, 'all_stations', 'all_stations.csv')
    save_path = os.path.join(smog_dir_path, 'smog_current', "smog_current.csv")
    history_dir = os.path.join(smog_dir_path, 'history')
    measures = {
        'PM10': ['PM10'],
        'PM25': ['PM25', 'PM2.5', 'PM2,5'],
    }

    ### LOAD HISTORICAL DATA
    #load_data(history_dir, stations_csv, measures)

    ### LOAD CURRENT DATA
    get_current_pollution(stations_csv, save_path, max_history=6)
