import polars as pl
import streamlit as st
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
    page_title="Product analysis", layout="wide", initial_sidebar_state="expanded"
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


def decrypt_data(data, key):
    cipher = AES.new(key, AES.MODE_CBC, iv=data[:16])
    pt = unpad(cipher.decrypt(data[16:]), AES.block_size)
    return pt


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
    col1, col2, col3, col4 = st.columns([4, 1, 1, 1])
    with col1:
        st.markdown("## Amazon analysis")
    with col2:
        gbp = st.number_input("GBP rate:", value=0.855)
    with col3:
        sek = st.number_input("SEK rate:", value=11.6)
    with col4:
        plz = st.number_input("PLZ rate:", value=4.29)
    st.divider()

    @st.cache_data
    def load_data(path):
        with open(path, "rb") as f:
            encrypted_data = f.read()
            buffer = io.BytesIO(decrypt_data(encrypted_data, key))
            df = pl.read_parquet(buffer)
        return df

    def calculate_price(row, gbp, plz):
        if row["country"] == "uk":
            return row["price"] / gbp
        elif row["country"] == "se":
            return row["price"] / sek
        elif row["country"] == "pl":
            return row["price"] / plz
        else:
            return row["price"]

    df = load_data("./data/Aen.parquet")
    vat = pl.DataFrame(
        {
            "country": ["de", "uk", "fr", "it", "se", "es", "pl"],
            "vat": [0.19, 0.2, 0.2, 0.22, 0.25, 0.21, 0.23],
        }
    )
    hnp = load_data("./data/hnp24.parquet")
    hnp.columns = [column.lower() for column in hnp.columns]
    hnp = hnp.with_columns(pl.col("article").cast(pl.Int32))
    amz = load_data("./data/Amz.parquet")

    df_s = df.join(
        hnp[["article", "hnp", "subcategory", "family", "product"]],
        on="article",
        how="left",
        # coalesce=True,
    ).with_columns(pl.col("date").cast(pl.Date))

    col1, col2, col3, col4 = st.columns([1, 1.5, 1, 1], gap="large")
    with col1:
        article = st.selectbox(
            "Select an article", df_s["article"].unique().sort().to_list(), index=1
        )
    with col2:
        pr_art = st.checkbox("Selection by product name", value=False)
        if not pr_art:
            product = df_s.filter(pl.col("article") == article)["product"].head(1)[0]
            st.success(product)
        else:
            prod = df_s["product"].unique().sort().to_list()
            product = st.selectbox("Select a product", prod, index=1)
            article1 = df_s.filter(pl.col("product") == product)["article"].head(1)[0]
            st.success(f"{article1}")
    with col3:
        date1 = st.date_input(
            "Select a date in a format YYYY/MM/DD",
            df_s["date"].max(),
            min_value=df_s["date"].min(),
        )
        previous_day = date1 - timedelta(days=1)
        previous_week = date1 - timedelta(weeks=1)
        previous_month = date1 - timedelta(days=30)

    with col4:
        margin_show = st.checkbox("Show margin", value=False)

    df_sp = df_s.filter(pl.col("product") == product).with_columns(
        pl.when(pl.col("country") == "uk")
        .then((pl.col("price") / gbp).round(2))
        .otherwise(
            pl.when(pl.col("country") == "se")
            .then((pl.col("price") / sek).round(2))
            .otherwise(
                pl.when(pl.col("country") == "pl")
                .then((pl.col("price") / plz).round(2))
                .otherwise(pl.col("price")),
            )
        )
        .alias("price_eur")
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
    df_spp = df_sp.filter(
        pl.col("date").is_in([date1, previous_day, previous_week, previous_month])
    ).join(
        amz.rename({"amz_price": "amazon"}),
        on="article",
        how="left",
        # coalesce=True,
    )
    df_spp = (
        df_spp.with_columns(
            margin=(1 - pl.col("amazon") / pl.col("price_net")).round(4),
            discount=(1 - pl.col("price_net") / pl.col("hnp")).round(4),
        )
        .unique()
        .sort(["country", "date"])
    )

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.1, row_heights=[0.4, 0.6]
    )

    df_latest = df_spp.filter(pl.col("date") == date1).with_columns(
        pl.when((pl.col("margin") * 100) < 15)
        .then(pl.lit("#ff0000"))
        .otherwise(
            pl.when((pl.col("margin") * 100) > 30)
            .then(pl.lit("#86d277"))
            .otherwise(pl.lit("#343499"))
        )
        .alias("color"),
    )

    if margin_show:
        annotations = []
        for i in range(len(df_latest)):
            annotations.append(
                dict(
                    x=df_latest["country"][i],
                    y=df_latest["margin"][i] * 100 + 8,
                    text=f"{df_latest['margin'][i] * 100:.1f}%",
                    showarrow=False,
                    font=dict(size=14, color=df_latest["color"][i]),
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=[df_latest["country"][i], df_latest["country"][i]],
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
                x=df_latest["country"],
                y=df_latest["margin"] * 100,
                mode="markers",
                name="Amazon margin",
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
                range=[-0.15, 80], showgrid=False
            ),  # Set y-axis range from 0 to 100
            annotations=annotations,  # Add annotations
            height=700,
        )
        fig.update_yaxes(title_text="Amazon Margin", row=1, col=1)
    else:
        pass

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
            row=2,
            col=1,
        )

    fig.update_yaxes(title_text="Price EUR", row=2, col=1)

    st.plotly_chart(fig, use_container_width=True)
    st.divider()

    if margin_show:
        col21, col22 = st.columns([1, 4], gap="large")
        with col21:
            country1 = st.selectbox(
                "Select a country", df_sp["country"].unique().sort().to_list(), index=0
            )
            st.divider()
            margin = st.slider(
                "Margin",
                min_value=15.0,
                max_value=40.0,
                value=20.0,
                step=1.0,
                format="%.1f%%",
            )
        with col22:
            vat1 = vat.filter(pl.col("country") == country1)["vat"].to_list()[0]
            df_corr = (
                df_s.filter(pl.col("country") == country1, pl.col("date") == date1)
                .with_columns(
                    pl.when(pl.col("country") == "uk")
                    .then((pl.col("price") / gbp).round(2))
                    .otherwise(
                        pl.when(pl.col("country") == "se")
                        .then((pl.col("price") / sek).round(2))
                        .otherwise(
                            pl.when(pl.col("country") == "pl")
                            .then((pl.col("price") / plz).round(2))
                            .otherwise(pl.col("price"))
                        )
                    )
                    .alias("price_eur")
                )
                .join(
                    amz.rename({"amz_price": "amazon"}),
                    on="article",
                    how="left",
                    # coalesce=True,
                )
                .with_columns(
                    margin=(
                        1 - pl.col("amazon") / (pl.col("price_eur") / (1 + vat1))
                    ).round(4)
                )
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
                    "price",
                    "country",
                    "date",
                    "hnp",
                    "subcategory",
                    "family",
                    "ancor",
                    "margin",
                )
            )

            title = f"Products with margin less than {margin}% in {country1} on {date1.strftime('%d.%m.%Y')} (quantity of products: {df_corr.height})"
            st.markdown(f"##### {title}")
            st.dataframe(df_corr, hide_index=True, use_container_width=True)
    else:
        pass
