import plotly.express as px
from dash import dcc, html, Dash
from flask import Flask
from pandas import DataFrame


def get_duration_histogram_app(data: DataFrame, server: Flask) -> Dash:
    app = Dash(__name__, server=server, routes_pathname_prefix='/dash/')

    fig = px.histogram(
        data,
        x="duration_ms",
        # range_x=[0, 15]
    )

    app.layout = html.Div(
        [
            dcc.Graph(figure=fig)
        ]
    )

    return app
