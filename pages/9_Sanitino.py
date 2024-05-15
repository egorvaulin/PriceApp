import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from middleware import authenticate_user
import toml
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import pyarrow.parquet as pq
import io

config = toml.load("./.streamlit/secrets.toml")
key = config["secrets"]["data_key"].encode("utf-8")


def decrypt_data(data, key):
    cipher = AES.new(key, AES.MODE_CBC, iv=data[:16])
    pt = unpad(cipher.decrypt(data[16:]), AES.block_size)
    return pt


# Page configuration
st.set_page_config(
    page_title="E-trader analysis", layout="wide", initial_sidebar_state="expanded"
)


# Change the font of the entire app
def set_font(font):
    st.markdown(
        f"""
                <style>
                body {{font-family: {font};}}
                </style>
                """,
        unsafe_allow_html=True,
    )


set_font("Arial")

# --- HIDE STREAMLIT STYLE ---
hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)

if authenticate_user():
    col1, col2, col3 = st.columns([4, 1, 1])
    with col1:
        st.markdown("## Sanitino analysis")
    with col2:
        czk = st.number_input("CZK rate:", value=25.5)
    with col3:
        ron = st.number_input("RON rate:", value=4.98)
    st.divider()

    @st.cache_data
    def load_data(path):
        with open(path, "rb") as f:
            encrypted_data = f.read()
            buffer = io.BytesIO(decrypt_data(encrypted_data, key))
            df = pd.read_parquet(buffer, engine="pyarrow")
        return df

    def calculate_price(row, czk, ron):
        if row["country"] == "cz":
            return row["price"] / czk
        elif row["country"] == "ro":
            return row["price"] / ron
        else:
            return row["price"]

    df = load_data("./data/Sen.parquet")
    vat = pd.DataFrame(
        {
            "country": ["de", "be", "cz", "fr", "it", "sk", "ro", "es"],
            "vat": [0.19, 0.21, 0.21, 0.2, 0.22, 0.20, 0.19, 0.21],
        }
    )
    vat.set_index("country", inplace=True)
    hnp = load_data("./data/hnp24.parquet")
    hnp.columns = hnp.columns.str.lower()
    rrp = load_data("./data/Rrp.parquet")
    rrp.columns = rrp.columns.str.lower()
    ancor = rrp[rrp["country"] == "an"]
    rrp = rrp[rrp["country"] != "an"]

    df_s = df.merge(
        hnp[["article", "hnp", "subcategory", "family", "product"]],
        on="article",
        how="left",
    )
    df_s["date"] = pd.to_datetime(df_s["date"]).dt.date

    subcat = df_s["subcategory"].unique()

    col1, col2, col3 = st.columns([1, 1.5, 1])
    with col1:
        subcategory = st.selectbox("Select a subcategory", subcat, index=6)
        df_s = df_s[df_s["subcategory"] == subcategory]

    with col2:
        prod = df_s["product"].unique()
        product = st.selectbox("Select a product", prod, index=0)

    with col3:
        date = st.date_input(
            "Select a date in a format YYYY/MM/DD",
            df_s["date"].max(),
            min_value=df_s["date"].min(),
        )
        previous_day = (date - pd.DateOffset(days=1)).date()
        previous_week = (date - pd.DateOffset(weeks=1)).date()
        previous_month = (date - pd.DateOffset(months=1)).date()

    df_sp = df_s[df_s["product"] == product]

    df_sp.loc[:, "price_eur"] = df_sp.apply(
        calculate_price, args=(czk, ron), axis=1
    ).round(2)
    rrp["price_eur"] = rrp.apply(calculate_price, args=(czk, ron), axis=1).round(2)
    rrp = rrp.merge(vat, left_on="country", right_index=True, how="left")
    rrp["price_eur"] = (rrp["price_eur"] / (1 + rrp["vat"])).round(2)

    df_sp = df_sp.merge(vat, left_on="country", right_index=True, how="left")
    df_sp["price_net"] = (df_sp["price_eur"] / (1 + df_sp["vat"])).round(2)
    df_sp.drop(columns=["price", "vat"], inplace=True)
    df_spp = df_sp[
        df_sp["date"].isin([date, previous_day, previous_week, previous_month])
    ].reset_index(drop=True)
    df_spp = df_spp.merge(
        ancor[["article", "price"]].rename(columns={"price": "ancor"}),
        on="article",
        how="left",
    )
    df_spp = df_spp.merge(
        rrp[["article", "country", "price_eur"]].rename(columns={"price_eur": "rrp"}),
        on=["country", "article"],
        how="left",
    )

    df_spp.loc[df_spp["country"] == "de", "rrp"] = (
        df_spp.loc[df_spp["country"] == "de", "rrp"]
        .fillna(df_spp["hnp"] / 1.19)
        .round(2)
    )

    df_spp["margin"] = (1 - df_spp["ancor"] / df_spp["price_net"]).round(4)
    df_spp["discount"] = (1 - df_spp["price_net"] / df_spp["rrp"]).round(4)
    df_stock = df_spp[df_spp["country"] == "de"].sort_values("date", ascending=True)[
        ["date", "stock"]
    ]
    df_spp.sort_values(["country", "date"], ascending=True, inplace=True)

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.1, row_heights=[0.4, 0.6]
    )

    df_latest = df_spp.loc[df_spp["date"] == date].copy()
    df_latest["color"] = "#343499"
    df_latest.loc[df_latest["margin"] * 100 < 45, "color"] = "#ff0000"
    df_latest.loc[df_latest["margin"] * 100 > 70, "color"] = "#86d277"
    annotations = []
    for i in range(len(df_latest)):
        annotations.append(
            dict(
                x=df_latest["country"].iloc[i],
                y=df_latest["margin"].iloc[i] * 100 + 8,
                text=f"{df_latest['margin'].iloc[i] * 100:.1f}%",
                showarrow=False,
                font=dict(size=14, color=df_latest["color"].iloc[i]),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=[df_latest["country"].iloc[i], df_latest["country"].iloc[i]],
                y=[0, df_latest["margin"].iloc[i] * 100],
                mode="lines",
                name="",
                line=dict(
                    color=df_latest["color"].iloc[i],
                    width=3,
                ),
                showlegend=False,
            )
        )
    fig.add_trace(
        go.Scatter(
            x=df_latest["country"],
            y=df_latest["margin"] * 100,
            mode="markers",
            name="Ancor+Sanitino margin",
            marker=dict(
                color=df_latest["color"],
                size=8,
            ),
            text=(df_latest["margin"] * 100).map("{:.1f}%".format),
            textposition="top center",  # Position text at the top of the markers
        ),
        row=1,
        col=1,
    )
    fig.update_layout(
        title=f"Prices by countries for {product}",
        xaxis_title=None,
        yaxis_title="Margin %",
        xaxis=dict(  # Modify x-axis labels
            tickfont=dict(size=16, color="#343499"),  # Increase font size
        ),
        legend=dict(
            yanchor="top",
            y=-0.2,  # Position legend below the graph
            xanchor="center",
            x=0.5,
            orientation="h",  # Horizontal orientation
            font=dict(size=16, color="#343499"),  # Increase font size
        ),
        yaxis=dict(range=[0, 100], showgrid=False),  # Set y-axis range from 0 to 100
        annotations=annotations,  # Add annotations
        height=700,
    )
    fig.update_yaxes(title_text="Ancor/Sanitino Margin", row=1, col=1)

    colors = ["#7d98a1", "#343499", "#9fb3ba", "#7676bb"]

    for day, color in zip(df_spp["date"].unique(), colors):
        df_date = df_spp[df_spp["date"] == day]
        formatted_date = day.strftime("%d %b")  # Format date as "day month"
        sorted_countries = sorted(
            df_date["country"]
        )  # Sort countries in alphabetical order
        fig.add_trace(
            go.Bar(
                x=sorted_countries,
                y=df_date["price_net"],
                name=f"Net price EUR on {formatted_date}",
                marker_color=color,
                text=df_date["price_net"].round(1).astype(str),  # Add values as text
                textposition="auto",  # Position text inside the bars
                textfont=dict(color="white"),  # Change text color to white
            ),
            row=2,
            col=1,
        )

    fig.update_yaxes(title_text="Price EUR", row=2, col=1)

    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    df_spd = df_sp[df_sp["date"] >= previous_month].sort_values("date", ascending=True)
    df_stock = df_spd[df_spd["country"] == "de"][["date", "stock"]]

    countries = df_spd["country"].unique()
    col11, col12 = st.columns([1, 6])
    with col11:
        try:
            countries_selected = st.multiselect(
                "Select countries", countries, default=["cz", "de", "es", "sk"]
            )
        except:
            pass
    with col12:
        colors = [
            "#7d98a1",
            "#343499",
            "#A54B2E",
            "#585858",
            "#86d277",
            "#F09577",
            "#8585BD",
            "#818080",
        ]
        fig_stock = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.1,
            row_heights=[0.3, 0.7],
        )
        fig_stock.add_trace(
            go.Bar(
                x=df_stock["date"],
                y=df_stock["stock"],
                name="Stock",
                marker_color="#5C5CA7",
                text=df_stock["stock"].astype(str),
                textposition="auto",
                textfont=dict(color="white", size=14),
            ),
            row=1,
            col=1,
        )
        try:
            for country, color in zip(
                countries_selected, colors[: len(countries_selected)]
            ):
                df_country = df_spd[df_spd["country"] == country]
                sorted_dates = sorted(
                    df_country["date"]
                )  # Sort dates in ascending order
                fig_stock.add_trace(
                    go.Scatter(
                        x=sorted_dates,
                        y=df_country["price_net"],
                        name=country,
                        marker_color=color,
                    ),
                    row=2,
                    col=1,
                )
        except:
            pass
        fig_stock.update_layout(
            title="Price by country and stock quantity over Time",
            xaxis_title=None,
            legend=dict(
                yanchor="top",
                y=-0.2,  # Position legend below the graph
                xanchor="center",
                x=0.5,
                orientation="h",  # Horizontal orientation
                font=dict(size=16, color="#343499"),  # Increase font size
            ),
            height=700,
        )
        fig_stock.update_yaxes(title_text="Stock", row=1, col=1)
        fig_stock.update_yaxes(
            title_text="Price by country and stock quantity over Time", row=2, col=1
        )
        st.plotly_chart(fig_stock, use_container_width=True)

st.divider()
col21, col22 = st.columns([1, 4], gap="large")
with col21:
    country1 = st.selectbox("Select a country", countries, index=0)
    st.divider()
    margin = st.slider(
        "Margin", min_value=25.0, max_value=50.0, value=40.0, step=1.0, format="%.1f%%"
    )

with col22:
    df_corr = df_s[df_s["country"] == country1]
    df_corr = df_corr[df_corr["date"] == date]
    df_corr = df_corr.merge(
        ancor[["article", "price"]].rename(columns={"price": "ancor"}),
        on="article",
        how="left",
    )
    df_corr["price_eur"] = df_corr.apply(
        calculate_price, args=(czk, ron), axis=1
    ).round(2)
    vat1 = vat.loc[country1, "vat"]
    df_corr["margin"] = (
        1 - df_corr["ancor"] / (df_corr["price_eur"] / (1 + vat1))
    ).round(4)
    df_corr = df_corr[df_corr["margin"] < margin / 100].copy()
    df_corr.loc[:, "article "] = df_corr["article"].astype(str).str.replace(",", "")
    df_corr = df_corr[
        ["article ", "product", "stock", "price_eur", "margin"]
    ].sort_values("margin", ascending=True)
    df_corr.loc[:, "price "] = (
        df_corr["price_eur"].round(1).astype(str).str.replace(".", ",")
    )
    df_corr.loc[:, "margin %"] = (df_corr["margin"] * 100).map("{:.1f}%".format)
    corr_len = len(df_corr)
    df_corr.drop(columns=["price_eur", "margin"], inplace=True)
    title = f"Products with margin less than {margin}% in {country1} on {date.strftime('%d.%m.%Y')} (quantity of products: {corr_len})"
    st.markdown(f"##### {title}")
    st.dataframe(df_corr, hide_index=True, use_container_width=True)
