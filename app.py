# -*- coding: utf-8 -*-

# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import calendar
from colors import darken
import dash
from dash_core_components import Checklist, Graph, Markdown, RadioItems, RangeSlider
from dash_html_components import *
from dash_bootstrap_components import Row, Col
from dash.dependencies import Input, Output
from dateutil.parser import parse
from month_colors import month_colors
import plotly.express as px
import pandas as pd
from re import fullmatch

from opts import opts


external_stylesheets = [
    'https://www.google-analytics.com/analytics.js',
    {
        'href': 'https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css',
        'rel': 'stylesheet',
        'integrity': 'sha384-MCw98/SFnGE8fJT3GXwEOngsV7Zt27NXFoaoApmYm81iuXoPkFOJwJ8ERdknLPMO',
        'crossorigin': 'anonymous'
    },
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
Prefix = 'ymrgt_cd/'  # year, month, region, gender, (user-)type; count, duration

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
df = df.groupby(['Month','Region','Gender','User Type',])[['Count','Duration',]].sum().reset_index()
print(f'Loaded {url}; {n} entries, cols: {df.columns}')


umos = df.Month.sort_values().drop_duplicates().reset_index(drop=True)
marks = umos.dt.strftime('%m/%y')
N = len(umos) - 1
month_to_idx = { v:k for k,v in marks.to_dict().items() }


def plot_months(
    df,
    title=None,
    name=None,
    stack_by='Gender',
    genders=None,
    y_col='Count',
    user_types=None,
    rolling_avgs=None,
    date_range=None,
    **kwargs,
):
    if stack_by == 'Gender':
        months = df.groupby(['Month','Gender'])[y_col].sum()
        idx = months.index.to_frame()
        month = idx.Month.dt.month
        year = idx.Month.dt.year
        stacked_key = idx.apply(lambda r: '%s, %s' % (calendar.month_abbr[r.Month.month], r.Gender), axis=1).rename('stacked_key')
    elif stack_by == 'User Type':
        months = df.groupby(['Month','User Type'])[y_col].sum()
        idx = months.index.to_frame()
        month = idx.Month.dt.month
        year = idx.Month.dt.year
        stacked_key = idx.apply(lambda r: '%s, %s' % (calendar.month_abbr[r.Month.month], r['User Type']), axis=1).rename('stacked_key')
    else:
        assert stack_by is None
        months = df.groupby(['Month'])[y_col].sum()
        idx = months.index.to_frame()
        month = idx.Month.dt.month
        year = idx.Month.dt.year
        stacked_key = idx.apply(lambda r: calendar.month_abbr[r.Month.month], axis=1).rename('stacked_key')

    m = month.rename('m')
    months = pd.concat([months, stacked_key, m, year.rename('y')], axis=1)

    # make months show up in input (and therefore legend) in order.
    # datetime column 'Month' ensures x-axis is still sorted chronologically
    p = months.reset_index()
    if stack_by == 'Gender':
        p.Gender = p.Gender.apply(lambda g: {'U':0,'M':2,'F':1}[g])
        p = p.sort_values(['m','Gender'])
    elif stack_by == 'User Type':
        p['User Type'] = p['User Type'].apply(lambda g: {'Customer':0,'Subscriber':1}[g])
        p = p.sort_values(['m','User Type'])
    else:
        assert stack_by is None
        p = p.sort_values('m')

    # Compute rolling avgs before any date-range restrictions below
    rolls = []
    if rolling_avgs:
        rolling_avgs = [ int(r) for r in rolling_avgs ]
        for r in rolling_avgs:
            k = f'{r}mo avg'
            rolling = p.groupby('Month')[y_col].sum().rolling(r).mean().rename(k)
            rolls.append(rolling)

    y_col_label = {'Count':'Total Rides','Duration':'Total Ride Minutes'}[y_col]

    if stack_by == 'Gender':
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
        labels={'stacked_key': 'Month, Gender',y_col:y_col_label}
    elif stack_by == 'User Type':
        color_sets = {
            'Subscriber': month_colors,
            'Customer': darken(month_colors, f=0.8),
        }
        color_sets = [
            v
            for k,v in color_sets.items()
            if not user_types or k in user_types
        ]
        color_discrete_sequence=[
            c
            for cc in zip(*color_sets)
            for c in cc
        ]
        labels={'stacked_key': 'Month, User Type',y_col:y_col_label}
    else:
        color_discrete_sequence = month_colors
        labels = {'stacked_key': 'Month',y_col:y_col_label}

    if date_range:
        start, end = date_range
        ums = umos.iloc[start:(end+1)]
        p = p.merge(ums, on='Month')
        rolls = [ r.reset_index().merge(ums, on='Month').set_index('Month')[r.name] for r in rolls ]

    mp = px.bar(
        p, x='Month', y=y_col, color='stacked_key',
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
    Output('date-range','value'),
    Input('graph','relayoutData'),
    Input('date-1yr','n_clicks'),
    Input('date-2yr','n_clicks'),
    Input('date-5yr','n_clicks'),
    Input('date-all','n_clicks'),
)
def _(relayoutData, n1, n2, n5, n_all,):
    ctx = dash.callback_context
    if ctx.triggered:
        prop_id = ctx.triggered[0]['prop_id'].split('.')[0]
        m = fullmatch(r'date-(?P<range>(?P<yrs>\d+)yr|all)', prop_id)
        if m:
            yrs = m['yrs']
            if yrs:
                mos = int(yrs) * 12
                return [N-mos+1, N]
            else:
                assert m['range'] == 'all'
                return [0, N]

    if relayoutData and 'xaxis.range[0]' in relayoutData:
        [start, end] = [ month_to_idx[parse(m).strftime('%m/%y')] for m in [relayoutData['xaxis.range[0]'], relayoutData['xaxis.range[1]']] ]
        return [start, end]
    else:
        return [0, N]


@app.callback(
    Output('graph','figure'),
    Input('region','value'),
    Input('stack-by','value'),
    Input('gender','value'),
    Input('user-type','value'),
    Input('date-range','value'),
    Input('y-col','value'),
)
def _(region, stack_by, genders, user_types, date_range, y_col):
    d = df.copy()
    if region == 'All':
        title = 'Monthly Citibike Rides'
    else:
        d = d[d.Region == region]
        title = f'Monthly Citibike{region} Rides'
    if genders:
        d = d[d.Gender.isin(genders)]
    if user_types == 'All':
        user_types = ['Subscriber','Customer',]
    else:
        d = d[d['User Type'] == user_types]
        user_types = [user_types]
    if stack_by == 'None':
        stack_by = None
    if stack_by and not stack_by in {'Gender','User Type'}:
        raise ValueError(f'Unrecognized `stack_by` value: {stack_by}')
    return plot_months(
        d, title=title,
        stack_by=stack_by,
        genders=genders,
        y_col=y_col,
        user_types=user_types,
        rolling_avgs=['12'],
        date_range=date_range,
    )


controls = {
    'Region': RadioItems(
        id='region',
        options=opts('All', 'NYC', 'JC'),
        value='All',
    ),
    'User Type': RadioItems(
        id='user-type',
        options=opts('All','Subscriber','Customer'),
        value='All',
    ),
    'Gender': Checklist(
        id='gender',
        options=opts({'Male':'M','Female':'F','Other / Unspecified':'U'}),
        value=['M','F','U',],
    ),
    'Count': RadioItems(
        id='y-col',
        options=opts({'Rides':'Count', 'Ride Minutes':'Duration'}),
        value='Count',
    ),
    # 'Time Window': RadioItems(
    #     id='time-window',
    #     options=opts(
    #         'Months',
    #         {'value':'Quarters','disabled':True},
    #         {'value':'Years','disabled':True},
    #     ),
    #     value='Months',
    # ),
    'Stack by': RadioItems(
        id='stack-by',
        options=opts(
            'Gender',
            'User Type',
            'None',
        ),
        value='None',
    ),
}

def icon(src, href, title):
    return A(
        [
            Img(
                src=f'/assets/{src}.png',
                className='icon',
            ),
        ],
        href=href,
        title=title,
    )

app.layout = Div([
    Graph(id='graph'),
    Row(
        [
            Col(
                [
                    Div(
                        [
                            'Date Range:',
                            Button( '1y', id='date-1yr',),
                            Button( '2y', id='date-2yr',),
                            Button( '5y', id='date-5yr',),
                            Button('All', id='date-all',),
                        ],
                        className='control-header',
                    ),
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
                [ Div(f'{label}:', className='control-header',), control, ],
                className='control',
            )
            for label, control in controls.items()
        ],
        className='no-gutters',
    ),
    Row(
        [
            Col([
                Div(
                    [
                        H2('About'),
                        Div([
                            Markdown(f'This plot should refresh when [new data is published by Citibike](https://www.citibikenyc.com/system-data) (typically around the 8th or 9th of each month, covering the previous month).'),
                            Markdown(f'Use the controls above to filter the plot by region, user type, gender, or date, group/stack by user type or gender, and toggle aggregation of rides or total ride minutes.'),
                        ]), Div([
                            'Code: ',icon('gh', 'https://github.com/neighbor-ryan/citibike#readme', 'GitHub logo'),' ',
                            'Data: ',icon('s3', 'https://s3.amazonaws.com/ctbk/index.html', 'Amazon S3 logo'),' ',
                            'Author: ',icon('twitter', 'https://twitter.com/RunsAsCoded', 'Twitter logo'),' ',
                        ]),
                    ],
                    className='footer',
                )
            ])
        ],
        className='no-gutters',
    )
])


if __name__ == '__main__':
    app.run_server(debug=True)
