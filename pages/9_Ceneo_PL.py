import polars as pl
import streamlit as st
import plotly.graph_objects as go
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

# if authenticate_user():
st.markdown("## Analysis of BLANCO prices on Ceneo")
st.divider()


@st.cache_data
def load_data(path):
    with open(path, "rb") as f:
        encrypted_data = f.read()
        buffer = io.BytesIO(decrypt_data(encrypted_data, key))
        df = pl.read_parquet(buffer)
    return df


def create_chart(df, title):  # Create a bar chart of the 'price' column
    smallest_price = df["price"].min()
    df = df.with_columns(surplus=pl.col("price") - smallest_price)
    chart = go.Figure(
        data=[
            go.Bar(
                x=df["shop"],
                y=[smallest_price] * len(df),
                name="price",
                marker_color="#d1d1e8",
            ),
            go.Bar(
                x=df["shop"], y=df["surplus"], name="Surplus", marker_color="#F09577"
            ),
        ]
    )

    chart.add_trace(
        go.Scatter(  # Add 'price' values at the bottom of the bars
            x=df["shop"],
            y=[0.1] * len(df),  # Set 'y' to a small number
            mode="text",  # Set the mode to 'text'
            text=[
                f"{val:.1f}" for val in df["price"]
            ],  # Set the 'text' to the 'price' values
            textposition="top center",  # Position the text at the top of the 'y' position
            textfont=dict(family="Arial", size=14, color="#575757"),
            showlegend=False,  # Do not show this trace in the legend
        )
    )

    chart.add_trace(
        go.Scatter(  # Add 'disc1' to the y2 axis with only markers
            x=df["shop"],
            y=df["disc1"],
            name="disc1",
            yaxis="y2",
            mode="markers+text",  # Add 'text' to the mode
            marker=dict(
                color="#343499",
                size=15,
                symbol="line-ew-open",
                line=dict(width=3),
            ),  # Increase the size of the markers
            text=[
                f"{val:.1%}" for val in df["disc1"]
            ],  # Format 'disc1' as a percentage with 1 decimal place
            textposition="top center",  # Position the text above the markers
            textfont=dict(family="Arial", size=14, color="#575757"),
        )
    )

    chart.update_layout(  # Update the layout to include the secondary y-axis and remove all gridlines
        title_text=title,
        height=600,
        barmode="stack",
        yaxis=dict(title="price", showgrid=False),
        yaxis2=dict(
            title="",
            overlaying="y",
            side="right",
            showticklabels=False,
            ticks="",
            showgrid=False,
            range=[0, max(df["disc1"]) + 0.05],
        ),  # Adjust the range for 'y2'
        xaxis=dict(showgrid=False),
        showlegend=False,  # Remove the legend
    )
    return chart


if authenticate_user():
    df = load_data("./data/Cen.parquet")
    hnp = load_data("./data/PL.parquet")
    hnp1 = load_data("./data/hnp24.parquet")
    hnp1.columns = [col.lower() for col in hnp1.columns]
    rrp = load_data("./data/rrp.parquet")

    df_de = (
        df.join(
            hnp,
            on="article",
            how="left",
        )
        .join(hnp1.select("article", "product"), on="article", how="left")
        .with_columns(
            disc1=1 - pl.col("price") / pl.col("hnp"),
        )
    )

    st.markdown("###### Select a product for analysis.")
    col1, col2 = st.columns([2, 5], gap="large")
    with col1:
        article = st.selectbox(
            "Select an article from the list",
            df_de["article"].unique().sort().to_list(),
            index=1,
        )
        pr_art = st.checkbox("Select product by product name", value=False)

        if not pr_art:
            product = df_de.filter(pl.col("article") == article)["product"].head(1)[0]
            st.success(product)
        else:
            product = st.selectbox(
                "Select a product from the list",
                df_de["product"].unique().sort().to_list(),
                index=1,
            )
            article1 = df_de.filter(pl.col("product") == product)["article"].head(1)[0]
            st.success(f"{article1}")

        st.divider()
        date1 = st.date_input(
            "Select a date",
            df_de["date"].max(),
            key="date_range1",
        )

    st.divider()

    with col2:
        df_de_prod = df_de.filter(pl.col("product") == product)
        df_sel_date = df_de_prod.filter(pl.col("date") == date1)

        graph1 = create_chart(
            df_sel_date.sort(by="disc1", descending=True).head(10),
            f"Shops and prices for {product} with discounts from HNP",
        )
        st.plotly_chart(graph1, use_container_width=True)

    st.divider()

    filt1_df = df_de.filter(pl.col("product") == product)

    col11, col12 = st.columns([1, 4], gap="large")
    with col11:
        multiselect_options = filt1_df["shop"].unique().sort().to_list()
        # Check if the default values exist in the options
        default_values = ["elektrohome.pl", "reuter.com", "fregadero.pl"]
        default_values = [
            shop for shop in default_values if shop in multiselect_options
        ][:2]
        # If no default values exist in the options, choose a different default value
        if not default_values and len(multiselect_options) > 0:
            default_values = [multiselect_options[0]]
        selected_shops = st.multiselect(
            "Select shops to compare", multiselect_options, default=default_values
        )
        filtered_df = filt1_df.filter(
            pl.col("shop").is_in(selected_shops)
        )  # Filter the data based on selected shops
        min_price_data = (
            filt1_df.sort("price")
            .group_by("date")
            .agg(
                pl.col("price").first().alias("min_price"),
                pl.col("shop").first().alias("min_price_shop"),
            )
            .sort("date")
        )
        mean_price_data = (
            filt1_df.group_by("date")
            .agg(pl.col("price").mean().round(1).alias("mean_price"))
            .sort("date")
        )

    with col12:
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
                    y=shop_data["price"].to_list(),
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
            title=f"<b>Price development for {product}</b>",
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
