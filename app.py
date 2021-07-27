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

external_scripts = [
    {'external_url': 'https://www.googletagmanager.com/gtag/js?id=G-YLWVNBWV51'},
    {'external_url': '/assets/ga.js',}
]

app = dash.Dash(
    __name__,
    title='Citibike Dashboard',
    external_stylesheets=external_stylesheets,
    external_scripts=external_scripts,
    serve_locally=False,
)
server = app.server

Bucket = 'ctbk'
Prefix = 'aggregated/ymrgtb_cd_201306:'  # ymrgtb_cd: (y)ear, (m)onth, (r)egion, (g)ender, user (t)ype, (b)ike ("rideable") type; (c)ount, (d)uration

from boto3 import client
from botocore.client import Config
s3 = client('s3', config=Config())
resp = s3.list_objects_v2(Bucket=Bucket, Prefix=Prefix)
contents = pd.DataFrame(resp['Contents'])
keys = contents.Key
keys = keys[keys.str.endswith('.parquet')]
key = keys.max()
url = f's3://{Bucket}/{key}'
print(f'Loading: {url}')
df = pd.read_parquet(url)
df['Gender'] = df.Gender.apply(lambda g: 'UMF'[g])
n = len(df)
df = df.groupby(['Month','Region','Gender','User Type','Rideable Type'])[['Count','Duration',]].sum().reset_index()
print(f'Loaded {url}; {n} entries, cols: {df.columns}')


umos = df.Month.sort_values().drop_duplicates().reset_index(drop=True)
marks = umos.dt.strftime('%m/%y')
N = len(umos) - 1
start_mo = N - 5*12 + 1
end_mo = N
month_to_idx = { v:k for k,v in marks.to_dict().items() }


def plot_months(
    df,
    title=None,
    name=None,
    stack_by='Gender',
    stack_relative=False,
    genders=None,
    rideable_types=None,
    y_col='Count',
    user_types=None,
    rolling_avgs=None,
    date_range=None,
    **kwargs,
):
    out_name = name

    stack_dicts = {
        'Gender': dict(vals=[ 'U', 'M', 'F', ], filter=genders),
        'Rideable Type': dict(vals=[ 'unknown', 'electric_bike', 'docked_bike', ], filter=rideable_types),
        'User Type': dict(vals=[ 'Customer', 'Subscriber', ], filter=user_types),
    }

    if date_range:
        start, end = date_range
        ums = umos.iloc[start:(end+1)]
    else:
        start, end = 0, -1

    colors = [ '#88aaff' ]

    if stack_by in stack_dicts:
        vals = stack_dicts[stack_by]['vals']
        months = df.groupby(['Month', stack_by])[y_col].sum()
        month_vals = pd.DataFrame([
            { 'Month': month, stack_by: val, }
            for month in umos.tolist()
            for val in vals
        ])
        month_vals = month_vals.set_index([ 'Month', stack_by, ])

        months = month_vals.merge(months, left_index=True, right_index=True, how='left').fillna(0)
        if stack_relative:
            month_totals = df.groupby(['Month'])[y_col].sum().rename('total')
            months = months.reset_index().merge(month_totals, left_on='Month', right_index=True, how='left').set_index(['Month', stack_by])
            months[y_col] = months[y_col] / months['total'] * 100
        idx = months.index.to_frame()
        month = idx.Month.dt.month
        year = idx.Month.dt.year
        stacked_key = idx.apply(lambda r: '%s, %s' % (calendar.month_abbr[r.Month.month], r[stack_by]), axis=1).rename('stacked_key')
    else:
        assert stack_by is None
        months = df.groupby(['Month'])[y_col].sum()
        month_vals = pd.DataFrame([
            { 'Month': month, }
            for month in umos.tolist()
        ])
        month_vals = month_vals.set_index([ 'Month', ])

        months = month_vals.merge(months, left_index=True, right_index=True, how='left').fillna(0)
        idx = months.index.to_frame()
        month = idx.Month.dt.month
        year = idx.Month.dt.year
        stacked_key = idx.apply(lambda r: calendar.month_abbr[r.Month.month], axis=1).rename('stacked_key')

    m = month.rename('m')
    months = pd.concat([ months, stacked_key, m, year.rename('y'), ], axis=1)

    # make months show up in input (and therefore legend) in order.
    # datetime column 'Month' ensures x-axis is still sorted chronologically
    p = months.reset_index()

    y_col_labels = {
        'Count': 'Total Rides',
        'Duration': 'Total Ride Minutes',
    }
    y_col_label = y_col_labels[y_col]

    # When stacking values, darken the lower values by these ratios
    color_set_fades = {
        2: [      .75, 1, ],
        3: [ .65, .80, 1, ],
    }

    if stack_by in stack_dicts:
        obj = stack_dicts[stack_by]
        vals = obj['vals']
        filter = obj['filter']
        val_to_ord = { val: idx for idx, val in enumerate(vals) }
        ord_to_val = { v: k for k, v in val_to_ord.items() }
        p[stack_by] = p[stack_by].apply(lambda v: val_to_ord[v])
        p = p.sort_values(['m',stack_by])
        p[stack_by] = p[stack_by].apply(lambda o: ord_to_val[o])

        color_fade_levels = color_set_fades[len(vals)]
        color_sets = {
            k: darken(
                colors, **(
                    dict()
                    if fade_level is None
                    else dict(f=fade_level)
                )
            )
            for k, fade_level
            in zip(vals, color_fade_levels)
        }
        color_sets = [
            v
            for k, v in color_sets.items()
            if not filter or k in filter
        ]
        color_discrete_sequence = [
            c
            for cc in zip(*color_sets)
            for c in cc
        ]
        labels = { 'stacked_key': f'Month, {stack_by}', y_col: y_col_label, }
    else:
        assert stack_by is None
        p = p.sort_values('m')
        color_discrete_sequence = colors
        labels = {'stacked_key': 'Month', y_col: y_col_label, }

    # Compute rolling avgs before any date-range restrictions below
    # TODO: only fill in `stack_by`-appropriate values?
    rolls = []
    roll_colors = {}
    roll_color_bases = {
        'U': '#'+'E'*6,
        'F': '#'+'D'*6,
        'M': '#'+'6'*6,
        'Subscriber': '#'+'7'*6,
        'Customer': '#'+'E'*6,
        'unknown': '#'+'E'*6,
        'electric_bike': '#'+'D'*6,
        'docked_bike': '#'+'6'*6,
    }
    roll_widths = {}
    roll_widths_bases = {
        3: 2,
        6: 3,
        12: 4,
    }
    if rolling_avgs:
        rolling_avgs = [ int(r) for r in rolling_avgs ]
        for r in rolling_avgs:
            if not (stack_relative and stack_by):
                k = f'{r}mo avg'
                rolling = p.groupby('Month')[y_col].sum().rolling(r).mean().rename(k)
                rolls.append(rolling)
                roll_colors[k] = 'black'
                roll_widths[k] = roll_widths_bases[r]
            if stack_by:
                partial_rolls = p.set_index(['Month',stack_by])[y_col].unstack().rolling(r).mean()
                if stack_by == 'Gender':
                    partial_rolls = partial_rolls[partial_rolls.index < pd.to_datetime('2021-02-01')]

                for k in partial_rolls:
                    name = f'{k} ({r}mo)'
                    v = partial_rolls[k].rename(name)
                    rolls.append(v)
                    #(roll_color_bases)
                    roll_colors[name] = roll_color_bases[k]
                    roll_widths[name] = roll_widths_bases[r]

    if stack_relative and stack_by:
        y_col_label += ' (%)'

    if date_range:
        p = p.merge(ums, on='Month')
        rolls = [ r.reset_index().merge(ums, on='Month').set_index('Month')[r.name] for r in rolls ]

    start, end = umos.iloc[start], umos.iloc[end]

    layout_kwargs = dict(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        yaxis_gridcolor='#DDDDDD',
        bargap=0,
        bargroupgap=0,
    )
    if stack_by and stack_relative:
        layout_kwargs['yaxis_range'] = [0, 100]

    mp = px.bar(
        p, x='Month', y=y_col, color=stack_by,
        color_discrete_sequence=color_discrete_sequence,
        labels=labels,
        # barmode='group',
        **kwargs,
    )

    if start.month > 1:
        start_year = start.year + 1
    else:
        start_year = start.year
    for year in range(start_year, end.year + 1):
        mp = mp.add_vline(pd.to_datetime(f'{year-1}-12-20'), line=dict(color='#EEEEEE'), layer='below',)
    for r in rolls:
        mp.add_scatter(
            x=r.index, y=r,
            #opacity=0.5,
            line=dict(
                color=roll_colors[r.name],
                width=roll_widths[r.name],
                # dash='dot',
            ),
            name=r.name,
        )
    if title:
        mp.update_layout(
            title={
                'text': title,
                'x':0.5,
                'xanchor':'center', 'yanchor':'top',
            },
            **layout_kwargs,
        )
    if out_name:
        mp.write_image(f'{out_name}.png')
        mp.write_image(f'{out_name}.svg')
    return mp


@app.callback(
    Output('date-range','value'),
    Input('graph','relayoutData'),
    Input('date-1yr','n_clicks'),
    Input('date-2yr','n_clicks'),
    Input('date-3yr','n_clicks'),
    Input('date-4yr','n_clicks'),
    Input('date-5yr','n_clicks'),
    Input('date-all','n_clicks'),
)
def _(relayoutData, n1, n2, n3, n4, n5, n_all,):
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
        return [start_mo, end_mo]


@app.callback(Output('date-1yr','disabled'), Input('date-range','value'))
def _(date_range):
    [ start, end ] = date_range
    return end == end_mo and start + 12 == end + 1


@app.callback(Output('date-2yr','disabled'), Input('date-range','value'))
def _(date_range):
    [ start, end ] = date_range
    return end == end_mo and start + 12*2 == end + 1


@app.callback(Output('date-3yr','disabled'), Input('date-range','value'))
def _(date_range):
    [ start, end ] = date_range
    return end == end_mo and start + 12*3 == end + 1


@app.callback(Output('date-4yr','disabled'), Input('date-range','value'))
def _(date_range):
    [ start, end ] = date_range
    return end == end_mo and start + 12*4 == end + 1


@app.callback(Output('date-5yr','disabled'), Input('date-range','value'))
def _(date_range):
    [ start, end ] = date_range
    return end == end_mo and start + 12*5 == end + 1


@app.callback(Output('date-all','disabled'), Input('date-range','value'))
def _(date_range):
    [ start, end ] = date_range
    return start == 0 and end == N


@app.callback(
    Output('graph','figure'),
    Input('region','value'),
    Input('stack-by','value'),
    Input('stack-percents','value'),
    Input('rolling-avgs','value'),
    Input('rideables','value'),
    Input('genders','value'),
    Input('user-type','value'),
    Input('date-range','value'),
    Input('y-col','value'),
)
def _(region, stack_by, stack_percents, rolling_avgs, rideables, genders, user_types, date_range, y_col):
    d = df.copy()
    if region == 'All':
        title = 'Monthly Citibike Rides'
    else:
        d = d[d.Region == region]
        title = f'Monthly Citibike{region} Rides'
    if rideables:
        if 'docked_bike' in rideables:
            # Pre 2021-02, all rides were labeled "classic_bike"; map those to the newer "docked_bike" designation here.
            # The "rideable type" data is generally pretty inaccurate; only a tiny number of rides are labeled
            # `electric_bike`, and all since the new data format ≥2021-02, even though there were many e-bike rides
            # earlier than that.
            rideables += ['classic_bike']
        d = d[d['Rideable Type'].isin(rideables)]
    if genders:
        d = d[d.Gender.isin(genders)]
    if user_types == 'All':
        user_types = ['Subscriber','Customer',]
    else:
        d = d[d['User Type'] == user_types]
        user_types = [user_types]
    stack_relative = bool(stack_percents)
    if stack_by == 'None':
        stack_by = None
    if stack_by and not stack_by in {'Gender','User Type','Rideable Type',}:
        raise ValueError(f'Unrecognized `stack_by` value: {stack_by}')
    return plot_months(
        d, title=title,
        stack_by=stack_by,
        stack_relative=stack_relative,
        genders=genders,
        rideable_types=rideables,
        y_col=y_col,
        user_types=user_types,
        rolling_avgs=rolling_avgs,
        date_range=date_range,
    )

rideable_type_opts = {
    # 'Classic': 'classic_bike',
    'Docked': 'docked_bike',
    'Electric': 'electric_bike',
    'Unknown': 'unknown',
}

gender_opts = {
    'Male': 'M',
    'Female': 'F',
    'Other / Unspecified': 'U',
}

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
    'Y-Axis': RadioItems(
        id='y-col',
        options=opts({
            'Rides':'Count',
            'Ride Minutes':'Duration',
        }),
        value='Count',
    ),
    'Rolling Avgs': Checklist(
        id='rolling-avgs',
        options=opts({
            '12mo':'12',
            '6mo':'6',
            '3mo':'3',
        }),
        value=['12'],
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
    'Stack by': [
        RadioItems(
            id='stack-by',
            options=opts({
                'User Type': 'User Type',
                'None': 'None',
                'Gender 🚧': 'Gender',
                'Rideable Type 🚧': 'Rideable Type',
            }),
            value='None',
        ),
        Checklist(
            id='stack-percents',
            options=opts({'Percentages':'T'}),
            value=[],
        )
    ],
    'Gender 🚧': Checklist(
        id='genders',
        options=opts(gender_opts),
        value=list(gender_opts.values()),
    ),
    'Rideable Type 🚧': Checklist(
        id='rideables',
        options=opts(rideable_type_opts),
        value=list(rideable_type_opts.values()),
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
                            Button( '3y', id='date-3yr',),
                            Button( '4y', id='date-4yr',),
                            Button( '5y', id='date-5yr',),
                            Button('All', id='date-all',),
                        ],
                        className='control-header',
                    ),
                    RangeSlider(
                        id='date-range',
                        min=0,
                        max=N,
                        value=[0, N],
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
                [ Div(f'{label}:', className='control-header',), ] + \
                [ Div(c, className='sub-control') for c in (control if isinstance(control, list) else [control]) ],
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
                            Markdown(f'Use the controls above to filter the plot by region, user type, gender, or date, group/stack by user type or gender, and toggle aggregation of rides or total ride minutes.'),
                            Markdown(f'This plot should refresh when [new data is published by Citibike](https://www.citibikenyc.com/system-data) (typically around the 2nd week of each month, covering the previous month).'),
                            Markdown(f'[The GitHub repo](https://github.com/neighbor-ryan/citibike) has more info as well as [planned enhancements](https://github.com/neighbor-ryan/citibike/issues).'),
                        ]),
                        H3('🚧 Known data-quality issues 🚧'),
                        Markdown('Several things changed in February 2021 (presumably when some backend systems were converted as part of [the Lyft acquistion](https://www.lyft.com/blog/posts/lyft-becomes-americas-largest-bikeshare-service)):'),
                        Markdown('''
                            - "Gender" information is no longer provided (it is present here through January 2021, after which point all rides are labeled "unknown")
                            - A new "Rideable Type" field was added, containing values `docked_bike` and `electric_bike` 🎉; however, it is mostly incorrect at present:
                              - Prior to February 2021, the field is absent (even though e-citibikes were in widespread use before then)
                              - Since February 2021, only a tiny number of rides are labeled `electric_bike` (122 in April 2021, 148 in May, 113 in June). This is certainly not accurate!
                                - One possibile explanation: [electric citibikes were launched in Jersey City and Hoboken around April 2021](https://www.hobokengirl.com/hoboken-jersey-city-citi-bike-share-program/); perhaps those bikes were part of a new fleet that show up as `electric_bike` in the data (where previous e-citibikes didn't).
                                - These `electric_bike` rides showed up in the default ("NYC") data, not the "JC" data, but it could be all in flux; February through April 2021 were also updated when the May 2021 data release happened in early June.                          
                            - The "User Type" values changed ("Subscriber" → "member", "Customer" → "casual"); I'm using the former/old values here, they seem equivalent.
                        '''),
                        Div([
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
