import streamlit as st
import polars as pl
from datetime import timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from middleware import authenticate_user
import toml
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import io

config = toml.load("./.streamlit/secrets.toml")
key = config["secrets"]["data_key"].encode("utf-8")


def decrypt_data(data, key):
    cipher = AES.new(key, AES.MODE_CBC, iv=data[:16])
    pt = unpad(cipher.decrypt(data[16:]), AES.block_size)
    return pt


# Page configuration
st.set_page_config(
    page_title="E-trader analysis", layout="wide", initial_sidebar_state="collapsed"
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
    col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 1])
    with col1:
        st.markdown("## Sanitino analysis")
    with col2:
        czk = st.number_input("CZK rate:", value=25.2)
    with col3:
        ron = st.number_input("RON rate:", value=4.98)
    with col4:
        plz = st.number_input("PLZ rate:", value=4.26)
    with col5:
        huf = st.number_input("HUF rate:", value=410.0)
    st.divider()

    @st.cache_data
    def load_data(path):
        with open(path, "rb") as f:
            encrypted_data = f.read()
            buffer = io.BytesIO(decrypt_data(encrypted_data, key))
            df = pl.read_parquet(buffer)
        return df

    def calculate_price(row, czk, ron, plz):
        if row["country"] == "cz":
            return row["price"] / czk
        elif row["country"] == "ro":
            return row["price"] / ron
        elif row["country"] == "pl":
            return row["price"] / plz
        elif row["country"] == "hu":
            return row["price"] / huf
        else:
            return row["price"]

    df = load_data("./data/Sen.parquet")
    df = df.with_columns(year=pl.col("date").dt.year())
    vat = pl.DataFrame(
        {
            "country": ["de", "be", "cz", "fr", "it", "sk", "ro", "es", "pl", "hu"],
            "vat": [0.19, 0.21, 0.21, 0.2, 0.22, 0.23, 0.19, 0.21, 0.23, 0.27],
        }
    )
    ancor = load_data("./data/an.parquet")
    ancor = ancor.with_columns(pl.col("article").cast(pl.Int32))

    df1 = (
        df.select("article")
        .unique()
        .join(
            ancor,
            on="article",
            how="left",
            # coalesce=True,
        )
        .select(["article", "product"])
        .unique(["article"])
        .sort("article")
    )
    articles = df1["article"].to_list()
    products = df1["product"].to_list()

    col1, col2, col3, col4 = st.columns([1, 1.5, 1, 1], gap="large")
    with col1:
        article = st.selectbox("Select an article", articles, index=1)

    with col2:
        pr_art = st.checkbox("Selection by product name", value=False)
        if not pr_art:
            product = df1.filter(pl.col("article") == article)["product"].head(1)[0]
            st.success(product)
        else:
            product = st.selectbox("Select a product", products, index=1)
            article = df1.filter(pl.col("product") == product)["article"].head(1)[0]
            st.success(f"{article}")
    with col3:
        date1 = st.date_input(
            "Select a date in a format YYYY/MM/DD",
            df["date"].max(),
            min_value=df["date"].min(),
        )
        previous_day = date1 - timedelta(days=1)
        previous_week = date1 - timedelta(weeks=1)
        previous_month = date1 - timedelta(days=30)

    with col4:
        margin_show = st.checkbox("Show margin", value=False)

    df_sp = (
        df.filter(pl.col("article") == article)
        .join(
            ancor,
            on=["article", "year"],
            how="left",
            # coalesce=True,
        )
        .with_columns(
            pl.when(pl.col("country") == "cz")
            .then((pl.col("price") / czk).round(2))
            .otherwise(
                pl.when(pl.col("country") == "ro")
                .then((pl.col("price") / ron).round(2))
                .otherwise(
                    pl.when(pl.col("country") == "pl")
                    .then((pl.col("price") / plz).round(2))
                    .otherwise(
                        pl.when(pl.col("country") == "hu")
                        .then((pl.col("price") / huf).round(2))
                        .otherwise(pl.col("price")),
                    )
                )
            )
            .alias("price_eur")
        )
    )

    df_sp = (
        df_sp.join(
            vat,
            on="country",
            how="left",
            #    coalesce=True
        )
        .with_columns(
            (pl.col("price_eur") / (1 + pl.col("vat"))).round(2).alias("price_net")
        )
        .drop("price", "vat")
    )

    df_spp = (
        df_sp.filter(
            pl.col("date").is_in([date1, previous_day, previous_week, previous_month])
        )
        .join(
            ancor[["article", "price", "year"]].rename({"price": "ancor"}),
            on=["article", "year"],
            how="left",
            # coalesce=True,
        )
        .unique(subset=["article", "country", "date"])
    )

    df_spp = df_spp.with_columns(
        margin=(1 - pl.col("ancor") / pl.col("price_net")).round(4),
        price_disc=(pl.col("price_eur") * (1 - pl.col("discount") / 100)).round(2),
        price_net_disc=(pl.col("price_net") * (1 - pl.col("discount") / 100)).round(2),
    )
    df_spp = df_spp.with_columns(
        margin_disc=(1 - pl.col("ancor") / pl.col("price_disc")).round(4),
    ).sort(["country", "date"])
    df_stock = (
        df_spp.filter(pl.col("country") == "de").sort("date").select(["date", "stock"])
    )

    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.4, 0.08, 0.52],
    )

    df_latest = df_spp.filter(pl.col("date") == date1).with_columns(
        pl.when((pl.col("margin") * 100) < 45)
        .then(pl.lit("#ff0000"))
        .otherwise(
            pl.when((pl.col("margin") * 100) > 70)
            .then(pl.lit("#86d277"))
            .otherwise(pl.lit("#343499"))
        )
        .alias("color"),
    )
    countries = df_latest["country"].unique().sort().to_list()
    country_map = {country: i for i, country in enumerate(countries)}
    df_latest = df_latest.with_columns(
        pl.col("country").map_elements(lambda x: country_map[x]).alias("country_id")
    )
    offset = 0.2

    if margin_show:
        annotations = []
        for i in range(len(df_latest)):
            annotations.append(
                dict(
                    x=df_latest["country_id"][i],
                    y=df_latest["margin"][i] * 100 + 8,
                    text=f"{df_latest['margin'][i] * 100:.1f}%",
                    showarrow=False,
                    font=dict(size=14, color=df_latest["color"][i]),
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=[df_latest["country_id"][i], df_latest["country_id"][i]],
                    y=[0, df_latest["margin"][i] * 100],
                    mode="lines",
                    name="",
                    line=dict(
                        color=df_latest["color"][i],
                        width=3,
                    ),
                    showlegend=False,
                )
            )
        fig.add_trace(
            go.Scatter(
                x=df_latest["country_id"],
                y=df_latest["margin"] * 100,
                mode="markers",
                name="Ancor+Sanitino margin",
                marker=dict(
                    color=df_latest["color"],
                    size=8,
                ),
                text=df_latest["margin"].map_elements(
                    lambda x: "{:.1f}%".format(x * 100), return_dtype=pl.Utf8
                ),
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
            yaxis=dict(
                range=[0, 100], showgrid=False
            ),  # Set y-axis range from 0 to 100
            annotations=annotations,  # Add annotations
            height=700,
        )
        fig.update_yaxes(title_text="Ancor/Sanitino Margin", row=1, col=1)
    else:
        pass

    text_list = []
    for i in range(len(df_latest)):
        text = f'<b>{df_latest["discount"][i]/100:.0%}</b> || {df_latest["price_disc"][i]:.1f} €'
        text_list.append(text)

    text_trace = go.Scatter(
        x=df_latest["country_id"],
        y=[0] * len(df_latest),
        mode="text",
        text=text_list,
        textposition="top center",
        textfont=dict(size=14, color="#343499", family="Arial"),
        showlegend=False,
    )
    fig.add_trace(text_trace, row=2, col=1)
    fig.update_yaxes(title_text="Discount", visible=False, row=2, col=1)

    colors = ["#7d98a1", "#343499", "#9fb3ba", "#7676bb"]

    for day, color in zip(df_spp["date"].unique().sort().to_list(), colors):
        df_date = df_spp.filter(pl.col("date") == day)
        formatted_date = day.strftime("%d %b")  # Format date as "day month"
        sorted_countries = sorted(
            df_date["country"]
        )  # Sort countries in alphabetical order
        fig.add_trace(
            go.Bar(
                x=sorted_countries,
                y=df_date["price_eur"],
                name=f"Price EUR on {formatted_date}",
                marker_color=color,
                text=df_date["price_eur"].round(1).cast(pl.Utf8),  # Add values as text
                textposition="auto",  # Position text inside the bars
                textfont=dict(color="white"),  # Change text color to white
            ),
            row=3,
            col=1,
        )

    fig.update_yaxes(title_text="Price EUR", row=3, col=1)

    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    df_spd = df_sp.filter(pl.col("date") >= previous_month).sort("date")
    df_stock = df_spd.filter(pl.col("country") == "de").select("date", "stock")

    col11, col12 = st.columns([1, 6])
    with col11:
        try:
            countries_selected = st.multiselect(
                "Select countries",
                df_spd["country"].unique().sort().to_list(),
                default=["cz", "de", "es", "sk"],
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
                text=df_stock.with_columns(pl.col("stock").cast(pl.Utf8))[
                    "stock"
                ].to_list(),
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
                df_country = df_spd.filter(pl.col("country") == country)
                sorted_dates = sorted(
                    df_country["date"]
                )  # Sort dates in ascending order
                fig_stock.add_trace(
                    go.Scatter(
                        x=sorted_dates,
                        y=df_country["price_eur"],
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
    if margin_show:
        col21, col22 = st.columns([1, 4], gap="large")
        with col21:
            country1 = st.selectbox(
                "Select a country", df_spd["country"].unique().sort().to_list(), index=0
            )
            st.divider()
            margin = st.slider(
                "Margin",
                min_value=25.0,
                max_value=50.0,
                value=40.0,
                step=1.0,
                format="%.1f%%",
            )

        with col22:
            vat1 = vat.filter(pl.col("country") == country1)["vat"].to_list()[0]
            df_corr = (
                df.filter(pl.col("country") == country1, pl.col("date") == date1)
                .with_columns(
                    pl.when(pl.col("country") == "cz")
                    .then((pl.col("price") / czk).round(2))
                    .otherwise(
                        pl.when(pl.col("country") == "ro")
                        .then((pl.col("price") / ron).round(2))
                        .otherwise(
                            pl.when(pl.col("country") == "pl")
                            .then((pl.col("price") / plz).round(2))
                            .otherwise(
                                pl.when(pl.col("country") == "hu")
                                .then((pl.col("price") / huf).round(2))
                                .otherwise(pl.col("price"))
                            )
                        )
                    )
                    .alias("price_eur")
                )
                .join(
                    ancor.rename({"price": "ancor"}),
                    on=["article", "year"],
                    how="left",
                    # coalesce=True,
                )
                .with_columns(
                    margin=(
                        1 - pl.col("ancor") / (pl.col("price_eur") / (1 + vat1))
                    ).round(4)
                )
                .unique(subset=["article", "country", "date"])
            )
            df_corr = (
                df_corr.filter(pl.col("margin") < margin / 100)
                .with_columns(
                    pl.col("article").cast(pl.Utf8).replace(",", "").alias("article"),
                    pl.col("price_eur").round(1).cast(pl.Utf8).replace(".", ","),
                )
                .sort("margin", descending=False)
                .with_columns(
                    pl.col("margin")
                    .map_elements(
                        lambda x: "{:.1f}%".format(x * 100), return_dtype=pl.Utf8
                    )
                    .alias("margin %")
                )
                .drop(
                    "price_eur",
                    "country",
                    "date",
                    "year",
                    "ancor",
                    "margin",
                )
            )

            title = f"Products with margin less than {margin}% in {country1} on {date1.strftime('%d.%m.%Y')} (quantity of products: {df_corr.height})"
            st.markdown(f"##### {title}")
            st.dataframe(df_corr, hide_index=True, use_container_width=True)
    else:
        pass
