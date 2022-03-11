import plotly.express as px
from dash import dcc, html, Dash
from flask import Flask
from pandas import DataFrame


def get_popularity_playing_app(data: DataFrame, server: Flask) -> Dash:
    app = Dash(__name__, server=server, routes_pathname_prefix='/dash2/')

    fig = px.scatter(
        data,
        x="popularity_mean",
        y="playing_count"
    )

    app.layout = html.Div(
        [
            dcc.Graph(figure=fig)
        ]
    )

    return app
