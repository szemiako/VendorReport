from data import Feeds
import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_table
from setup import DataFrame
from setup import datetime

def get_progress(df: DataFrame) -> int:
    sent = len(df[df['Status'] == 'Sent'])
    not_sent = len(df[df['Status'] == 'Not Sent'])
    total = sent + not_sent
    return int(sent / total * 100)

def generate_table(df: DataFrame, max_rows: int=0) -> html.Table:
    return html.Table(
        [html.Tr([html.Th(col) for col in df.columns])] +

        [html.Tr([
            html.Td(df.iloc[i][col]) for col in df.columns
        ]) for i in range(max(max_rows, len(df)))]
    )

def makey_layout():
    '''Make the layout of the page.'''
    progress_section = html.Div(children=[
        html.H4(f'Progress of Feeds (as-of {CURRENT_TIME})'),
        html.Div(children=[
                dbc.Progress(children='{0}%'.format(PROGRESS), value=PROGRESS, id='progress', style={'height': '30px'}),
            ]
        )], style={'padding': 10}
    )

    table1_section = dash_table.DataTable(
        id='datatable-feeds',
        columns=[
            {'name': i, 'id': i} for i in data.feeds.columns
        ],
        filter_action='native',
        sort_action='native',
        sort_mode='multi',
        page_size=50,
        fixed_rows={'headers': True},
        style_table={
            'height': '300px',
            'overflowY': 'auto',
            'overflowX': 'scroll'
        },
        style_cell={
            'height': 'auto',
            'minWidth': '60px',
            'whitespace': 'normal'
        },
        data=data.feeds.to_dict('records')
    )

    table2_section = html.Div(children=[
        html.H4('Explore by vendor...', id='t_head2'),
        dcc.Dropdown(id='dropdown', options=[
            {'label': i, 'value': i} for i in data.all_actuals['vendor_name'].unique()
        ], multi=False, placeholder='Explore by vendor...'),
        html.Div(id='table-container')
    ])

    return html.Div([
        progress_section,
        table1_section,
        table2_section
    ])

EXTERNAL_STYLESHEETS=['https://codepen.io/chriddyp/pen/bWLwgP.css', dbc.themes.BOOTSTRAP]
CURRENT_TIME = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
data = Feeds()
PROGRESS = get_progress(data.feeds)

app = dash.Dash(__name__, external_stylesheets=EXTERNAL_STYLESHEETS)
app.layout = makey_layout()

@app.callback(
    Output('table-container', 'children'),
    [Input('dropdown', 'value')]
)
def display_table(dropdown_value: str) -> html.Table:
    if dropdown_value is None:
        return generate_table(DataFrame(columns=data.all_actuals.columns))

    dff = data.all_actuals[data.all_actuals['vendor_name'] == dropdown_value]
    return generate_table(dff)

if __name__ == '__main__':
    app.run_server(debug=True, port=9995)