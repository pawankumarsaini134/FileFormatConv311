import json
import pandas as pd
import uuid
import os
import glob
from dask import dataframe as dd



def get_colums(ds):
    with open('data/retail_db/schemas.json') as fp:
        schemas = json.load(fp)
    try:
        schema=schemas.get(ds)
        if not schema:
            raise KeyError
        cols=sorted(schema, key=lambda x: x['column_position'])
        columns=[col['column_name'] for col in cols]
        return columns
    except KeyError:
        print(f'Schema not found for {ds}')
        return None
def main():
    for path in glob.glob('data/retail_db/*'):
        if os.path.isdir(path):
            ds=os.path.split(path)[1]
            for file in glob.glob(f'{path}/*'):
                df=pd.read_csv(file,names=get_colums(ds))
                os.makedirs(f'data/retail_demo/{ds}',exist_ok=True)
                df.to_json(
                    f'data/retail_demo/{ds}/{uuid.uuid4()}.json',
                    orient='records',
                    lines=True
                    )
                print(f'Number of records Processed for {os.path.split(file)[1]} in {ds} is {df.shape}')
def main_nyse():
    print('File format conversion started')

    df = dd.read_csv(
        'data/nyse_all/nyse_data/NYSE*.txt.gz',
        names=['ticker', "trade_date","open_price","low_price","high_price","close_price",'volume'],
        blocksize=None
        )
    print('Dataframe created and JSON conversion started')
    df.to_json(
        'data/nyse_all/nyse_json/part-*.json.gz',
        orient='records',
        lines=True,
        compression='gzip',
        name_function=lambda i: '%05d' % i
)
    print('File format conversion completed')

if __name__ == "__main__":
    #print(get_colums('departments'))
    #main()
    main_nyse()
