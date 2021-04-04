# -*- coding: utf-8 -*-

# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import calendar
from colors import darken
import dash
from dash_core_components import Graph, RadioItems, RangeSlider, Checklist
from dash_html_components import *
from dash_bootstrap_components import Row, Col
from dash.dependencies import Input, Output
from month_colors import month_colors
import plotly.express as px
import pandas as pd

from opts import opts


external_stylesheets = [
    'https://www.google-analytics.com/analytics.js',
    {
        'href': 'https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css',
        'rel': 'stylesheet',
        'integrity': 'sha384-MCw98/SFnGE8fJT3GXwEOngsV7Zt27NXFoaoApmYm81iuXoPkFOJwJ8ERdknLPMO',
        'crossorigin': 'anonymous'
    },
    'https://codepen.io/chriddyp/pen/bWLwgP.css',
]

app = dash.Dash(__name__, title='Citibike Dashboard', external_stylesheets=external_stylesheets)
server = app.server

# app.scripts.config.serve_locally = False
# app.scripts.append_script({
#     'external_url': 'https://www.googletagmanager.com/gtag/js?id=G-JDP856VCQP'
# })
# app.scripts.append_script({
#     'external_url': './ga.js'
# })

Bucket = 'ctbk'
Prefix = 'ymrgt_c'

from boto3 import client
from botocore.client import Config
s3 = client('s3', config=Config())
resp = s3.list_objects_v2(Bucket=Bucket, Prefix=Prefix)
contents = pd.DataFrame(resp['Contents'])
keys = contents.Key
key = keys.max()
url = f's3://{Bucket}/{key}'
print(f'Loading: {url}')
df = pd.read_parquet(url)
df['Gender'] = df.Gender.apply(lambda g: 'UMF'[g])
n = len(df)
df = df.groupby(['Month','Region','Gender'])['Count'].sum().reset_index()
print(f'Loaded {url}; {n} entries, cols: {df.columns}')


def plot_months(
    df,
    title=None,
    name=None,
    gender_stack=True,
    genders=None,
    rolling_avgs=None,
    date_range=None,
    **kwargs,
):
    if gender_stack:
        months = df.groupby(['Month','Gender'])['Count'].sum()
        idx = months.index.to_frame()
        month = idx.Month.dt.month
        year = idx.Month.dt.year
        mg = idx.apply(lambda r: '%s, %s' % (calendar.month_abbr[r.Month.month], r.Gender), axis=1).rename('mg')
    else:
        months = df.groupby(['Month'])['Count'].sum()
        idx = months.index.to_frame()
        month = idx.Month.dt.month
        year = idx.Month.dt.year
        mg = idx.apply(lambda r: calendar.month_abbr[r.Month.month], axis=1).rename('mg')

    m = month.rename('m')
    months = pd.concat([months, mg, m, year.rename('y')], axis=1)

    # make months show up in input (and therefore legend) in order.
    # datetime column 'Month' ensures x-axis is still sorted chronologically
    p = months.reset_index()
    if gender_stack:
        p.Gender = p.Gender.apply(lambda g: {'U':0,'M':2,'F':1}[g])
        p = p.sort_values(['m','Gender'])
    else:
        p = p.sort_values('m')

    # Compute rolling avgs before any date-range restrictions below
    rolls = []
    if rolling_avgs:
        rolling_avgs = [ int(r) for r in rolling_avgs ]
        for r in rolling_avgs:
            k = f'{r}mo avg'
            rolling = p.groupby('Month').Count.sum().rolling(r, min_periods=1).mean().rename(k)
            rolls.append(rolling)

    if gender_stack:
        color_sets = {
            'U': month_colors,
            'F': darken(month_colors, f=0.85),
            'M': darken(month_colors, f=0.75),
        }
        color_sets = [
            v
            for k,v in color_sets.items()
            if not genders or k in genders
        ]
        color_discrete_sequence=[
            c
            for cc in zip(*color_sets)
            for c in cc
        ]
        labels={'mg': 'Month, Gender','Count':'Number of Rides'}
    else:
        color_discrete_sequence = darken(month_colors, f=0.9)
        labels = {'mg': 'Month','Count':'Number of Rides'}

    if date_range:
        start, end = date_range
        ums = umos.iloc[start:(end+1)]
        p = p.merge(ums, on='Month')
        rolls = [ r.reset_index().merge(ums, on='Month').set_index('Month')[r.name] for r in rolls ]

    mp = px.bar(
        p, x='Month', y='Count', color='mg',
        color_discrete_sequence=color_discrete_sequence,
        labels=labels,
        **kwargs,
    )
    for r in rolls:
        mp.add_scatter(x=r.index, y=r, line=dict(color='black',), name=r.name)
    if title:
        mp.update_layout(
            title={
                'text': title,
                'x':0.5,
                'xanchor':'center', 'yanchor':'top',
            }
        )
    if name:
        mp.write_image(f'{name}.png')
        mp.write_image(f'{name}.svg')
    return mp


@app.callback(
    Output('graph','figure'),
    Input('region','value'),
    Input('stack-by','value'),
    Input('gender','value'),
    Input('date-range','value'),
)
def _(region, stack_by, genders, date_range):
    d = df.copy()
    if region == 'All':
        title = 'Monthly Citibike Rides'
    else:
        d = d[d.Region == region]
        title = f'Monthly Citibike{region} Rides'
    if genders:
        d = d[d.Gender.isin(genders)]
    if stack_by == 'Gender':
        gender_stack = True
    elif stack_by == 'None':
        gender_stack = False
    else:
        raise ValueError(stack_by)
    return plot_months(
        d, title=title,
        gender_stack=gender_stack,
        genders=genders,
        rolling_avgs=['12'],
        date_range=date_range,
    )


umos = df.Month.sort_values().drop_duplicates().reset_index(drop=True)
marks = umos.apply(lambda d: "%d/%s" % (d.month, str(d.year)[-2:]))

controls = {
    'Region': RadioItems(
        id='region',
        options=opts('All', 'NYC', 'JC'),
        value='All',
    ),
    'Time Window': RadioItems(
        id='time-window',
        options=opts(
            'Months',
            {'value':'Quarters','disabled':True},
            {'value':'Years','disabled':True},
        ),
        value='Months',
    ),
    'Stack by': RadioItems(
        id='stack-by',
        options=opts(
            'Gender',
            {'value':'Ride Type','disabled':True},
            'None',
        ),
        value='Gender',
    ),
    'Gender': Checklist(
        id='gender',
        options=opts({'Male':'M','Female':'F','Other / Unspecified':'U'}),
        value=['M','F','U',],
    ),
}
app.layout = Div([
    Graph(id='graph'),
    Row(
        [
            Col(
                [
                    Div('Date Range:', className='control-header',),
                    RangeSlider(
                        id='date-range',
                        min=0,
                        max=len(umos) - 1,
                        value=[0, len(umos) - 1],
                        marks={
                            d['index']: {
                                'label': (
                                    d['Month']
                                    if (int(d['Month'].split('/')[0]) % 3) == (umos.dt.month.values[0] % 3) or d['index'] + 1 == len(umos)
                                    else ''
                                ),
                                'style': {
                                    'text-align': 'right',
                                    'transform': 'translate(0,1.5em) rotate(-90deg)',
                                    'transform-origin': '0 50% 0',
                                }
                            }
                            for d in marks.reset_index().to_dict('records')
                        },
                    ),
                ],
                className='control',
            ),
        ],
        className='no-gutters',
    ),
    Row(
        [
            Col(
                [
                    Div(f'{label}:', className='control-header',),
                    control,
                ],
                className='control',
            )
            for label, control in controls.items()
        ],
        className='no-gutters',
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)
