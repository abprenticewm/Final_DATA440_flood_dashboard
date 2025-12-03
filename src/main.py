'''
main.py: dashboard app for visualizing Virginia flood risk using USGS stream gauge data.

Displays real-time data on an interactive map with gauge details.

Features:
- Map of Virginia with gauges color-coded by flow rate changes.
- Clickable gauges for detailed time-series data.
'''

import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc, Output, Input, State
import webbrowser
from threading import Timer
import os
import numpy as np
import update_pipeline
import tzlocal
from pathlib import Path

# define paths
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
DATA_FILE = DATA_DIR / "gauge_data_processed.csv"

# create Dash app
app = Dash(__name__, suppress_callback_exceptions=True)
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

# build map figure
def build_map(df):
    df = df[df["flow_cfs"].notna() & (df["flow_cfs"] >= 0)].copy()

    # round ROC to 3 decimals for hover display
    df["pct_change_3h"] = df["pct_change_3h"].round(3)

    # assign unique ID for each gauge
    df["site_id"] = df.index

    # formatted pct change for hover
    df["pct_display_hover"] = df["pct_change_3h"].apply(
        lambda x: "Missing Data" if pd.isna(x) else f"{x:+.3f} %"
    )

    # color logic
    def color_logic(x):
        if pd.isna(x):
            return "#808080"  # gray
        elif x <= 0:
            return "#A18F65"  # brown
        elif x > 25:
            return "#942719"  # red
        else:
            return "#5279A8"  # blue

    df["color_group"] = df["pct_change_3h"].apply(color_logic)

    # size scaling
    def size_class(flow):
        if flow <= 50:
            return 10
        elif flow <= 200:
            return 20
        else:
            return 30

    df["size_class"] = df["flow_cfs"].apply(size_class)

    # flow status
    df["status"] = np.where(
        (df["p90_flow_cfs"].notna()) & (df["flow_cfs"] >= df["p90_flow_cfs"]),
        "HIGH FLOW",
        "Normal Flow"
    )

    # center map
    center_lat = df["latitude"].mean()
    center_lon = df["longitude"].mean()

    fig = px.scatter_map(
        df,
        lat="latitude",
        lon="longitude",
        color="color_group",
        size="size_class",
        hover_name="site_name",
        hover_data={},
        custom_data=[
            "site_id",
            "site_name",
            "flow_cfs",
            "p90_flow_cfs",
            "pct_display_hover", 
            "status"
        ],
        zoom=6,
        center={"lat": center_lat, "lon": center_lon},
        height=700,
        color_discrete_map={
            "#808080": "#808080",
            "#A18F65": "#A18F65",
            "#942719": "#942719",
            "#5279A8": "#5279A8"
        }
    )

    fig.update_traces(
        hovertemplate=
        "<b>%{customdata[1]}</b><br>" +
        "Current Flow: %{customdata[2]:.3f} cfs<br>" +
        "3-Hour ROC: %{customdata[4]}<br>" +
        "Status: %{customdata[5]}<br>" +
        "<extra></extra>"
    )

    fig.update_layout(showlegend=False, margin=dict(l=0, r=0, t=0, b=0))
    return fig

# main map layout
def main_map_layout():
    df = pd.read_csv(DATA_FILE)
    return html.Div([
        # entire page container
        html.Div([
            html.Div([
                # sidebar content
                html.H1(
                    "VA Flood Risk Map",
                    style={"textAlign": "center", "marginTop": "10px", "color": "white"}
                ),
                html.Hr(style={"borderColor": "white"}),
                html.H3("About This Map", style={"color": "white"}),
                html.P(
                    "This dashboard maps current flood risk across Virginia using real-time "
                    "USGS stream gauge data. The newest available data loads automatically "
                    "when the dashboard opens.",
                    style={"fontSize": "14px", "color": "white"}
                ),
                html.Hr(style={"borderColor": "white"}),
                html.Button(
                    "Refresh Data",
                    id="refresh-btn",
                    n_clicks=0,
                    style={
                        "display": "block",
                        "margin": "10px auto",
                        "padding": "10px 20px",
                        "fontSize": "16px",
                        "backgroundColor": "#D3D3D3",
                        "color": "black",
                        "border": "none",
                        "borderRadius": "4px",
                        "cursor": "pointer"
                    }
                ),
                html.P(
                    "Click to manually refresh data. Values may not change if USGS has not "
                    "published a newer reading yet.",
                    style={"fontSize": "13px", "textAlign": "center", "marginTop": "5px", "color": "white"}
                ),
                html.Hr(style={"borderColor": "white"}),
                html.H3("Legend", style={"color": "white"}),
                html.P("Flow Trend (3-hour % change):", style={"marginBottom": "4px", "color": "white"}),
                html.Ul([
                    html.Li("Brown  — flow stable or decreasing (≤ 0%)", style={"color": "white"}),
                    html.Li("Blue  — rising moderately (0% to 25%)", style={"color": "white"}),
                    html.Li("Red  — sharp rise (> 25%)", style={"color": "white"}),
                    html.Li("Gray  — missing data", style={"color": "white"}),
                ], style={"fontSize": "13px"}),
                html.Br(),
                html.P("Flow in cubic feet per second (cfs):", style={"marginBottom": "4px", "color": "white"}),
                html.Ul([
                    html.Li("Small dot — 0 to 50 cfs", style={"color": "white"}),
                    html.Li("Medium dot — 51 to 200 cfs", style={"color": "white"}),
                    html.Li("Large dot — above 200 cfs", style={"color": "white"})
                ], style={"fontSize": "13px"}),
                html.Hr(style={"borderColor": "white"}),
                html.H3("How to Use", style={"color": "white"}),
                html.Ul([
                    html.Li("Hover a gauge to view summary statistics.", style={"color": "white"}),
                    html.Li("Click a gauge to open a detailed page with more data.", style={"color": "white"}),
                ], style={"fontSize": "13px"}),
            ],
            style={
                "width": "20%",
                "minWidth": "200px",
                "background": "#1A1F4B",
                "padding": "15px",
                "overflowY": "auto",
                "boxSizing": "border-box",
            }),
            html.Div([
                dcc.Graph(
                    id="map-graph",
                    figure=build_map(df),
                    style={"height": "100%", "width": "100%"}
                )
            ],
            style={
                "flex": "1",
                "display": "flex",
                "flexDirection": "column",
                "overflow": "hidden"
            }),
        ],
        style={
            "display": "flex",
            "height": "100vh",
            "overflow": "hidden"
        })
    ])

# page routing
@app.callback(
    Output('page-content', 'children'),
    Input('url', 'pathname')
)
def display_page(pathname):
    df_main = pd.read_csv(DATA_FILE)

    if pathname == '/':
        return main_map_layout()

    elif pathname.startswith('/gauge/'):
        site_id = int(pathname.split('/')[-1])
        site_no = df_main.loc[site_id]["site_no"]

        # select the row by site_no for latest data
        site_row = df_main[df_main["site_no"] == site_no].iloc[0]

        site_name = site_row["site_name"]
        flow_cfs = site_row["flow_cfs"]
        p90_flow = site_row["p90_flow_cfs"]
        pct_change_3h = site_row["pct_change_3h"]
        latitude = site_row["latitude"]
        longitude = site_row["longitude"]

        if pd.isna(pct_change_3h):
            title_color = "#5A4A2F"
            pct_display = "Missing Data"
        else:
            pct_display = f"{pct_change_3h:.3f}%"
            if pct_change_3h <= 0:
                title_color = "#A18F65"
            elif pct_change_3h > 25:
                title_color = "#942719"
            else:
                title_color = "#5279A8"

        # read full time-series CSV
        ts = pd.read_csv(DATA_DIR / "gauge_data.csv")
        ts["timestamp_utc"] = pd.to_datetime(ts["timestamp_utc"])
        local_tz = tzlocal.get_localzone()
        if ts["timestamp_utc"].dt.tz is None:
            ts["timestamp_local"] = ts["timestamp_utc"].dt.tz_localize('UTC').dt.tz_convert(local_tz)
        else:
            ts["timestamp_local"] = ts["timestamp_utc"].dt.tz_convert(local_tz)

        gauge_df = ts[ts["site_no"] == site_no].sort_values("timestamp_local")

        # 6-hour window
        if not gauge_df.empty:
            latest_time = gauge_df["timestamp_local"].max()
            cutoff = latest_time - pd.Timedelta(hours=6)
            gauge_6h = gauge_df[gauge_df["timestamp_local"] >= cutoff]
        else:
            gauge_6h = gauge_df.copy()

        fig = px.line(
            gauge_6h,
            x="timestamp_local",
            y="flow_cfs",
            title=f"{site_name}",
            labels={"timestamp_local": f"Time ({str(local_tz)})", "flow_cfs": "Flow (cfs)"}
        )
        fig.update_layout(title={'x':0.5, 'xanchor': 'center'}, height=500, margin=dict(l=20, r=20, t=40, b=20))

        # render page
        return html.Div([
            # header and stats
            html.Div([
                html.H1(f"{site_name}", style={"textAlign": "center", "marginTop": "15px", "color": title_color}),
                html.Div(f"Site {site_no} | Latitude: {latitude}° | Longitude: {longitude}°",
                         style={"textAlign": "center", "fontWeight": "bold", "marginBottom": "15px", "fontSize": "15px"}),

                html.Div(
                    style={
                        "display": "flex",
                        "flexDirection": "row",
                        "justifyContent": "center",
                        "marginBottom": "15px",
                        "flexWrap": "wrap",
                        "fontSize": "15px",
                    },
                    children=[
                        html.Div(
                            style={
                                "lineHeight": "1.2",
                                "textAlign": "center",
                                "flex": "1",
                                "maxWidth": "300px",
                            },
                            children=[
                                html.H4("Flow Stats", style={"color": "#5279A8", "marginBottom": "5px"}),
                                html.P([
                                    f"Status: {'HIGH FLOW' if flow_cfs >= p90_flow else 'Normal'} | Flow: {flow_cfs:.3f} cfs",
                                    html.Br(),
                                    f"3h ROC: {pct_display}"
                                ], style={"margin": "2px 0"}),
                                html.P(f"Threshold (90th percentile): {f'{p90_flow:.3f}' if not pd.isna(p90_flow) else 'Missing Data'} cfs",
                                       style={"margin": "2px 0"}
                                       ),
                            ]
                        ),
                    ]
                ),

                # explanation box
                html.Div(
                    style={
                        "flex": "1",
                        "minWidth": "300px",
                        "lineHeight": "1.5",
                        "marginTop": "15px",
                        "marginBottom": "20px",
                        "textAlign": "left"
                    },
                    children=[
                        html.H4("Explanation", style={"color": "#5279A8", "marginBottom": "10px"}),
                        html.P([
                            html.B("Rate of Change"),
                            " compares the current flow to previous measurement approx. 3 hours ago."
                        ], style={"marginBottom": "5px"}),
                        html.P([
                            "The ", html.B("90th percentile high flow threshold"),
                            " is calculated from ~20 years of historical USGS data for this calendar day. "
                            "If the current flow exceeds this threshold, the gauge is classified as HIGH FLOW."
                        ], style={"marginBottom": "5px"}),
                    ]
                )
            ], style={"display": "flex", "flexDirection": "column", "alignItems": "center"}),

            # 6-hour Graph
            dcc.Graph(id="gauge-timeseries", figure=fig, style={"width": "90%", "margin": "0 auto"}),

            # download buttons
            html.Div(
                style={
                    "display": "flex",
                    "justifyContent": "space-evenly",
                    "alignItems": "center",
                    "margin": "15px 0",
                    "flexWrap": "wrap",
                    "gap": "10px",
                },
                children=[
                    html.Button(
                        "Download Graph (PNG)",
                        id="download-graph-btn",
                        n_clicks=0,
                        style={"padding": "10px 20px", "fontSize": "16px", "cursor": "pointer"}
                    ),
                    html.Button(
                        "Download Full CSV (All Data for This Site)",
                        id="download-fullcsv-btn",
                        n_clicks=0,
                        style={"padding": "10px 20px", "fontSize": "16px", "cursor": "pointer"}
                    ),
                ]
            ),

            # hidden download components
            dcc.Download(id="download-graph-file"),
            dcc.Download(id="download-fullcsv-file"),

            # notes section - bubbles
            html.Div([
                html.Div([
                    html.H4("Missing Data"),
                    html.P(
                        "Missing data is recorded by the USGS as -9999. In this dashboard -9999 is converted to NaN and displayed as missing data."
                    )
                ], style={
                    "borderRadius": "50%",
                    "padding": "20px",
                    "margin": "10px",
                    "flex": "1",
                    "background": "#d6cfbf",
                    "textAlign": "center"
                }),
                html.Div([
                    html.H4("Negative Flow"),
                    html.P(
                        "Negative flow rates can occur in tidal areas where water reverses "
                        "direction during high tide and temporarily flows upstream."
                    )
                ], style={
                    "borderRadius": "50%",
                    "padding": "20px",
                    "margin": "10px",
                    "flex": "1",
                    "background": "#d6cfbf",
                    "textAlign": "center"
                })
            ], style={
                "display": "flex",
                "flexDirection": "row",
                "justifyContent": "space-between",
                "width": "90%",
                "margin": "20px auto",
                "flexWrap": "wrap"
            })
        ])
    else:
        return html.H1("404: Page not found")

# generate file names for downloads
def unique_filename(base_name, ext):
    folder = ROOT / "download_data"
    folder.mkdir(parents=True, exist_ok=True)
    date_str = pd.Timestamp.utcnow().strftime("%Y%m%d")
    n = 1
    while True:
        filename = f"{base_name}_{date_str}_{n}.{ext}"
        path = folder / filename
        if not path.exists():
            return str(path)
        n += 1

# refresh map callback
@app.callback(
    Output("map-graph", "figure"),
    Input("refresh-btn", "n_clicks")
)
def update_map(n_clicks):
    update_pipeline.main()
    df = pd.read_csv(DATA_FILE)
    return build_map(df)

# gauge click callback
@app.callback(
    Output('url', 'pathname'),
    Input('map-graph', 'clickData'),
    prevent_initial_call=True
)
def go_to_gauge(clickData):
    if clickData:
        site_id = clickData['points'][0]['customdata'][0]
        return f'/gauge/{site_id}'
    return '/'

# download graph callback
@app.callback(
    Output("download-graph-file", "data"),
    Input("download-graph-btn", "n_clicks"),
    State("gauge-timeseries", "figure"),
    State("url", "pathname"),
    prevent_initial_call=True
)
def download_graph(n_clicks, fig, pathname):
    site_id = int(pathname.split("/")[-1])
    df_main = pd.read_csv(DATA_FILE)
    site_name = df_main.loc[site_id]["site_name"].replace(" ", "_")
    filepath = unique_filename(site_name, "png")
    import plotly.io as pio
    pio.write_image(fig, filepath, scale=2)
    return dcc.send_file(filepath)

# download full csv callback
@app.callback(
    Output("download-fullcsv-file", "data"),
    Input("download-fullcsv-btn", "n_clicks"),
    State("url", "pathname"),
    prevent_initial_call=True
)
def download_full_csv(n_clicks, pathname):
    site_id = int(pathname.split("/")[-1])
    df_main = pd.read_csv(DATA_FILE)
    site_no = df_main.loc[site_id]["site_no"]
    site_name = df_main.loc[site_id]["site_name"].replace(" ", "_")
    ts = pd.read_csv(DATA_DIR / "gauge_data.csv")
    ts["timestamp_utc"] = pd.to_datetime(ts["timestamp_utc"])
    gauge_df = ts[ts["site_no"] == site_no].sort_values("timestamp_utc")
    filepath = unique_filename(site_name, "csv")
    gauge_df.to_csv(filepath, index=False)
    return dcc.send_file(filepath)

# auto-open browser
def open_browser():
    webbrowser.open_new("http://127.0.0.1:8050/")

# main
if __name__ == "__main__":
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        update_pipeline.main()
        Timer(1, open_browser).start()
    app.run(debug=True, port=8050)