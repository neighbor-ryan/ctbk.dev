# -*- coding: utf-8 -*-

# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import calendar
from colors import darken
import dash
from dash_core_components import Graph, RadioItems, Checklist
from dash_html_components import *
from dash_bootstrap_components import Row, Col
from dash.dependencies import Input, Output
from month_colors import month_colors
import plotly.express as px
import pandas as pd
from utz import sxs


external_stylesheets = [
    {
        'href': 'https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css',
        'rel': 'stylesheet',
        'integrity': 'sha384-MCw98/SFnGE8fJT3GXwEOngsV7Zt27NXFoaoApmYm81iuXoPkFOJwJ8ERdknLPMO',
        'crossorigin': 'anonymous'
    },
    'https://codepen.io/chriddyp/pen/bWLwgP.css',
]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

df = pd.read_parquet('year-month-region-gender-weekday.parquet')
df['Gender'] = df.Gender.apply(lambda g: 'UMF'[g])


def plot_months(df, title=None, name=None, gender_stack=True, genders=None, **kwargs):
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
    months = sxs(months, mg, m, year.rename('y'))

    # make months show up in input (and therefore legend) in order.
    # datetime column 'Month' ensures x-axis is still sorted chronologically
    p = months.reset_index()
    if gender_stack:
        p.Gender = p.Gender.apply(lambda g: {'U':0,'M':2,'F':1}[g])
        p = p.sort_values(['m','Gender'])
    else:
        p = p.sort_values('m')

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
        mp = px.bar(
            p,
            x='Month',
            y='Count',
            color='mg',
            color_discrete_sequence=color_discrete_sequence,
            labels={'mg': 'Month, Gender','Count':'Number of Rides'},
            **kwargs,
        )
    else:
        mp = px.bar(
            p, x='Month', y='Count', color='mg',
            color_discrete_sequence=darken(month_colors, f=0.9),
            labels={'mg': 'Month','Count':'Number of Rides'},
            **kwargs,
        )
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
    Input('gender-stack','value'),
    Input('gender','value'),
)
def _(region, gender_stack, genders):
    if region == 'All':
        d = df
    else:
        d = df[df.Region == region]
    if genders:
        d = d[d.Gender.isin(genders)]
    return plot_months(d, gender_stack=gender_stack, genders=genders, title=f'Monthly Citibike{region} Rides')


controls = {
    'Region': RadioItems(
        id='region',
        options=[{'label': region, 'value': region} for region in ['All','NYC','JC']],
        value='All',
    ),
    'Stack by gender': RadioItems(
        id='gender-stack',
        options=[{'label':'Yes','value':'True'},{'label':'No','value':''}],
        value='True',
    ),
    'Gender': Checklist(
        id='gender',
        options=[
            {'label':'Male','value':'M'},
            {'label':'Female','value':'F'},
            {'label':'Unspecified','value':'U'},
        ],
        value=['M','F','U',],
    ),
}
app.layout = Div([
    Graph(id='graph'),
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
