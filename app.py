from datetime import datetime, timedelta
import sqlite3
import json

from flask import Flask, request
import pandas as pd
import requests


app = Flask(__name__)

# example query:
# http://127.0.0.1:5000/?end_time_str=2023-02-24T11:21:00&start_time_str=2023-02-24T08:35:00


@app.route('/')
def hello_world():  # put application's code here
    download_and_store_data()
    start_time_str = request.args.get('start_time_str')
    end_time_str = request.args.get('end_time_str')
    return get_data(start_time_str, end_time_str)


def download_and_store_data():
    url = 'https://services.swpc.noaa.gov/json/rtsw/rtsw_mag_1m.json'
    response = requests.get(url)
    data = response.json()

    conn = sqlite3.connect('swpx_data.db')
    df = pd.DataFrame(data)
    df.to_sql('swpx_data', conn, if_exists='replace')
    conn.close()


def get_data(start_time_str, end_time_str):
    conn = sqlite3.connect('swpx_data.db')
    _start_time = datetime.strptime(start_time_str, '%Y-%m-%dT%H:%M:%S')
    _end_time = datetime.strptime(end_time_str, '%Y-%m-%dT%H:%M:%S')
    diff = timedelta(minutes=5)
    loop_end = _start_time + diff
    output = []
    while _start_time <= _end_time:
        query = f"""
            SELECT
                AVG(bz_gsm) as bz_gsm_avg,
                AVG(bx_gsm) as bx_gsm_avg,
                AVG(by_gsm) as by_gsm_avg
            FROM
                swpx_data
            WHERE
                time_tag BETWEEN ? AND ?
        """
        _cursor = conn.cursor()
        res = _cursor.execute(
            query,
            (
                _start_time.strftime('%Y-%m-%dT%H:%M:%S'),
                loop_end.strftime('%Y-%m-%dT%H:%M:%S')
            )
        ).fetchall()

        print(_start_time, loop_end)
        print(res)
        output.append({
            'start_time': _start_time.strftime('%Y-%m-%dT%H:%M:%S'),
            'end_time': loop_end.strftime('%Y-%m-%dT%H:%M:%S'),
            'bz_gsm_avg': res[0][0],
            'bx_gsm_avg': res[0][1],
            'by_gsm_avg': res[0][2],
        })

        _start_time += diff
        loop_end += diff
    conn.close()
    return output


if __name__ == '__main__':
    app.run()
    # download_and_store_data()
    # get_data('2023-02-23T11:23:00', '2023-02-24T11:21:00')
