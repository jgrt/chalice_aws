from chalice import Chalice, Rate
import pandas as pd
import requests
from datetime import datetime
import  io
from chalicelib import FP

app = Chalice(app_name='helloworld')

assets = {
    'data': {'codec': 'json'},
    'model': {'codec': 'pickle'},
    'prediction': {'codec': 'json'}
}
asset_prefix = "s3://covid-dataset/test_dir/"

USER = "dev-user"

read_data = lambda x: (
    pd
    .read_csv(x)
    .drop(columns=["Province/State", "Lat", "Long"])
    .rename(columns = {"Country/Region" : "country"})
    .groupby("country")
    .sum()
)

def read_file(cases):
    df = pd.DataFrame()
    for case_type in cases:
        url = f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-{case_type}.csv"
        data = io.StringIO(requests.get(url).text)
        _df = read_data(data)
        _df.columns = pd.MultiIndex.from_product([[case_type], _df.columns.tolist()])
        if df.empty:
            df = _df
        else:
            df = df.join(_df)
    return df


@app.schedule(Rate(24, unit=Rate.HOURS))
def save_data(event):
    cases = ["Confirmed", "Recovered", "Deaths"]
    df = read_file(cases)
    dff = (
        df
            .reset_index()
            .melt(id_vars=["country"])
            .rename(columns={"variable_0": "case", "variable_1": "date"})
    )
    data = dff.to_dict("records")
    fp = FP(assets=assets, uri=asset_prefix)
    timestamp = str(datetime.now())
    path = fp.save(data=data, user=USER, name=timestamp, asset="data")
    return path

@app.route('/')
def index():
    return {'timestamp': str(datetime.now())}
