# -*- coding: utf-8 -*-

"""etl for bp--energy dataset. More info refer to README of the repo."""

import pandas as pd
import numpy as np
import os
import xlrd
from ddf_utils.str import to_concept_id

# Configuration
source = '../source/bp-statistical-review-of-world-energy-2015-workbook.xlsx'
out_dir = '../../'


def extract_datapoint(data, ddf_id):
    data = data.drop(['2013.1', 'of total'], axis=1)
    data = data.set_index('geo')

    d = data.T.unstack()
    d = d.dropna()
    d = d.reset_index()
    d.columns = ['geo', 'year', ddf_id]

    return d.sort_values(by=['geo', 'year'])


def preprocess(data):
    data = data.rename(columns={data.columns[0]: 'geo_name'})
    data['geo'] = data['geo_name'].map(to_concept_id)
    data = data.set_index('geo')
    data = data.dropna(how='all')
    data = data[:'total_world']
    data = data.reset_index()
    return data


if __name__ == '__main__':

    from functools import partial
    from ddf_utils.index import create_index_file

    imported = []
    not_imported = []

    sheets = xlrd.open_workbook(source).sheet_names()

    to_concept_id_ = partial(to_concept_id, sub='[/ -\.\*–";]+')
    concepts_ids = list(map(to_concept_id_, sheets))

    concept_dict = dict(zip(sheets, concepts_ids))

    geos = []

    for i in sheets:
        print('running tab '+i+'...')
        if 'Regional' in i or 'Trade' in i or 'Price' in i:
            continue
        try:
            data = pd.read_excel(source, na_values=['n/a'], sheetname=i, skiprows=2)
            data = preprocess(data)
            df = extract_datapoint(data.drop('geo_name', axis=1), concept_dict[i])
            geos.append(data[['geo', 'geo_name']].copy())
        except Exception as e:
            # print(e)
            not_imported.append(i)
            continue

        fn = os.path.join(out_dir, 'ddf--datapoints--'+concept_dict[i]+'--by--geo--year.csv')
        df.to_csv(fn, index=False)
        imported.append(i)

    geo_df = pd.concat(geos)
    geo_df['geo_name'] = geo_df['geo_name'].str.strip()
    geo_df = geo_df.drop_duplicates()
    fn_geo = os.path.join(out_dir, 'ddf--entities--geo.csv')
    geo_df.to_csv(fn_geo, index=False)

    concepts_df = pd.DataFrame([], columns=['concept', 'concept_type', 'name'])

    concept_dict['Geo'] = 'geo'
    concept_dict['Geo Name'] = 'geo_name'
    concept_dict['Name'] = 'name'

    imported.append('Geo')
    imported.append('Geo Name')
    imported.append('Name')

    imported_dict = dict([[k, concept_dict[k]] for k in imported])

    concepts_df['concept'] = imported_dict.values()
    concepts_df['name'] = [x.strip() for x in imported_dict.keys()]
    concepts_df['concept_type'] = 'measure'

    concepts_df = concepts_df.set_index('concept')
    concepts_df.loc['geo', 'concept_type'] = 'entity_domain'
    concepts_df.loc['name', 'concept_type'] = 'string'

    fn_concept = os.path.join(out_dir, 'ddf--concepts.csv')
    concepts_df.to_csv(fn_concept)

    create_index_file(out_dir)

    print('Done.')
