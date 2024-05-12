import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
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
    page_title="Price development", layout="wide", initial_sidebar_state="expanded"
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
    st.markdown("## Price development")
    st.divider()

    @st.cache_data
    def load_data(path):
        with open(path, "rb") as f:
            encrypted_data = f.read()
            buffer = io.BytesIO(decrypt_data(encrypted_data, key))
            df = pd.read_parquet(buffer, engine="pyarrow")
        return df

    df = load_data("./data/Ien.parquet")
    df_de = df[df["country"] == "de"]
    df_fr = df[df["country"] == "fr"]
    hnp = load_data("./data/hnp24.parquet")
    hnp.columns = hnp.columns.str.lower()

    df_de = df_de.merge(
        hnp[["article", "hnp", "subcategory", "family", "product"]],
        on="article",
        how="left",
    )
    df_de.drop(columns=["country"], inplace=True)

    col1, col2 = st.columns(2)  # Set up 2 columns for user input
    with col1:
        selectbox_options = df_de["product"].unique()
        selected_product = st.selectbox("Select an article", selectbox_options, index=0)

    filt1_df = df_de[df_de["product"] == selected_product]
    with col2:
        multiselect_options = np.sort(filt1_df.shop.unique())
        # Check if the default values exist in the options
        default_values = ["Amazon", "sanitino.de", "sonono.de"]
        default_values = [
            shop for shop in default_values if shop in multiselect_options
        ][:2]
        # If no default values exist in the options, choose a different default value
        if not default_values and multiselect_options.size > 0:
            default_values = [multiselect_options[0]]
        selected_shops = st.multiselect(
            "Select shops to compare", multiselect_options, default=default_values
        )

    with_delivery = st.checkbox("Show prices with delivery", value=False)
    if with_delivery:
        column = "price_delivery"
    else:
        column = "price"

    mask = filt1_df["shop"].isin(selected_shops)
    filtered_df = filt1_df.loc[mask].copy()  # Filter the data based on selected shops
    filtered_df.loc[:, "date"] = pd.to_datetime(
        filtered_df["date"]
    )  # Convert the date column to datetime

    min_price_data = filt1_df.groupby("date").agg(
        min_price=("price", "min"),
        min_price_shop=(
            "shop",
            "first",
        ),  # This assumes the shop name is in the same row as the minimum price
    )
    mean_price_data = filt1_df.groupby("date").agg(mean_price=("price", "mean"))
    mean_price = mean_price_data["mean_price"]
    min_price = min_price_data["min_price"]
    min_price_shop = min_price_data["min_price_shop"]
    # Create a line plot
    fig = go.Figure()
    colors_p = [
        "#7d98a1",
        "#343499",
        "#fbe059",
        "#86d277",
        "#9fb3ba",
        "#7676bb",
        "#fbe572",
        "#a1dd96",
    ]  # Add more colors if needed

    for i, shop in enumerate(selected_shops):
        shop_data = filtered_df[filtered_df["shop"] == shop]
        fig.add_trace(
            go.Scatter(
                x=shop_data["date"],
                y=shop_data[column],
                name=shop,
                mode="lines",
                line=dict(color=colors_p[i]),
            )
        )
    # Add minimum price line
    fig.add_trace(
        go.Scatter(
            x=min_price.index,
            y=min_price,
            mode="lines",
            name="Minimum Price",
            line=dict(dash="dash", color="#818080"),
            text=min_price_shop,  # Add shop name as hover text
            hoverinfo="text+y",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=mean_price.index,
            y=mean_price,
            mode="lines",
            name="Mean Price",
            line=dict(
                dash="dash", color="#FF6133"
            ),  # Choose a different color for the mean price line
        )
    )
    fig.update_layout(
        xaxis_title=None,
        yaxis_title="<b>Price</b>",
        title=f"<b>Price development for {selected_product}</b>",
        legend=dict(
            yanchor="top",
            y=-0.2,  # Position legend below the graph
            xanchor="center",
            x=0.5,
            orientation="h",  # Horizontal orientation
            font=dict(size=16, color="#343499"),  # Increase font size
        ),
    )
    st.plotly_chart(fig, use_container_width=True)
