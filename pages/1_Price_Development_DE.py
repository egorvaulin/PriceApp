import polars as pl
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
    st.markdown("## Price development Germany")
    st.divider()

    @st.cache_data
    def load_data(path):
        with open(path, "rb") as f:
            encrypted_data = f.read()
            buffer = io.BytesIO(decrypt_data(encrypted_data, key))
            df = pl.read_parquet(buffer)
        return df

    df = load_data("./data/Ien.parquet")
    hnp = load_data("./data/hnp24.parquet")
    hnp.columns = [col.lower() for col in hnp.columns]

    df_de = (
        df.filter(pl.col("country") == "de")
        .join(
            hnp.select(pl.col("article", "hnp", "subcategory", "family", "product")),
            on="article",
            how="left",
        )
        .drop("country")
    )

    col1, col2 = st.columns(2)  # Set up 2 columns for user input
    with col1:
        selectbox_options = df_de["product"].unique().sort().to_list()
        selected_product = st.selectbox("Select an article", selectbox_options, index=0)

    filt1_df = df_de.filter(pl.col("product") == selected_product)
    with col2:
        multiselect_options = filt1_df["shop"].unique().sort().to_list()
        # Check if the default values exist in the options
        default_values = ["Amazon", "sanitino.de", "sonono.de"]
        default_values = [
            shop for shop in default_values if shop in multiselect_options
        ][:2]
        # If no default values exist in the options, choose a different default value
        if not default_values and len(multiselect_options) > 0:
            default_values = [multiselect_options[0]]
        selected_shops = st.multiselect(
            "Select shops to compare", multiselect_options, default=default_values
        )

    with_delivery = st.checkbox("Show prices with delivery", value=False)
    if with_delivery:
        column = "price_delivery"
    else:
        column = "price"

    filtered_df = filt1_df.filter(
        pl.col("shop").is_in(selected_shops)
    )  # Filter the data based on selected shops

    min_price_data = (
        filt1_df.sort(column)
        .group_by("date")
        .agg(
            pl.col(column).first().alias("min_price"),
            pl.col("shop").first().alias("min_price_shop"),
        )
        .sort("date")
    )
    mean_price_data = (
        filt1_df.group_by("date")
        .agg(pl.col(column).mean().round(1).alias("mean_price"))
        .sort("date")
    )
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
        shop_data = filtered_df.filter(pl.col("shop") == shop)
        fig.add_trace(
            go.Scatter(
                x=shop_data["date"].to_list(),
                y=shop_data[column].to_list(),
                name=shop,
                mode="lines",
                line=dict(color=colors_p[i]),
            )
        )
    # Add minimum price line
    fig.add_trace(
        go.Scatter(
            x=min_price_data["date"].to_list(),
            y=min_price_data["min_price"].to_list(),
            mode="lines",
            name="Minimum Price",
            line=dict(dash="dash", color="#818080"),
            text=min_price_data[
                "min_price_shop"
            ].to_list(),  # Add shop name as hover text
            hoverinfo="text+y",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=mean_price_data["date"].to_list(),
            y=mean_price_data["mean_price"].to_list(),
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
