import polars as pl
import streamlit as st
import plotly.graph_objects as go
from middleware import authenticate_user
from datetime import timedelta
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
    page_title="Product analysis", layout="wide", initial_sidebar_state="collapsed"
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


def create_chart(df, title):  # Create a bar chart of the 'price' column
    smallest_price = df[column2].min()
    df = df.with_columns(surplus=pl.col(column2) - smallest_price)
    chart = go.Figure(
        data=[
            go.Bar(
                x=df["shop"],
                y=[smallest_price] * len(df),
                name=column2,
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
                f"{val:.1f}" for val in df[column2]
            ],  # Set the 'text' to the 'price' values
            textposition="top center",  # Position the text at the top of the 'y' position
            textfont=dict(family="Arial", size=14, color="#575757"),
            showlegend=False,  # Do not show this trace in the legend
        )
    )

    chart.add_trace(
        go.Scatter(  # Add 'disc1' to the y2 axis with only markers
            x=df["shop"],
            y=df[column],
            name=column,
            yaxis="y2",
            mode="markers+text",  # Add 'text' to the mode
            marker=dict(
                color="#343499",
                size=15,
                symbol="line-ew-open",
                line=dict(width=3),
            ),  # Increase the size of the markers
            text=[
                f"{val:.1%}" for val in df[column]
            ],  # Format 'disc1' as a percentage with 1 decimal place
            textposition="top center",  # Position the text above the markers
            textfont=dict(family="Arial", size=14, color="#575757"),
        )
    )

    chart.update_layout(  # Update the layout to include the secondary y-axis and remove all gridlines
        title_text=title,
        height=600,
        barmode="stack",
        yaxis=dict(title=column2, showgrid=False),
        yaxis2=dict(
            title="",
            overlaying="y",
            side="right",
            showticklabels=False,
            ticks="",
            showgrid=False,
            range=[0, max(df[column]) + 0.05],
        ),  # Adjust the range for 'y2'
        xaxis=dict(showgrid=False),
        showlegend=False,  # Remove the legend
    )
    return chart


if authenticate_user():
    st.markdown("## Product analysis Germany")
    st.divider()

    @st.cache_data
    def load_data(path):
        with open(path, "rb") as f:
            encrypted_data = f.read()
            buffer = io.BytesIO(decrypt_data(encrypted_data, key))
            df = pl.read_parquet(buffer)
        return df

    df = load_data("./data/Ien.parquet")
    df = df.with_columns(pl.col("year").cast(pl.Int32))
    hnp = load_data("./data/tlp.parquet")
    hnp = hnp.with_columns(pl.col("article").cast(pl.Int32),
                           pl.col("year").cast(pl.Int32))

    df_de = (
        df.filter(pl.col("country") == "de")
        .drop("country")
        .with_columns(year=pl.col("date").dt.year())
    )

    df1 = (
        df_de.select(pl.col("article"))
        .unique()
        .sort("article")
        .join(
            hnp.select(pl.col("article", "product")),
            on="article",
            how="left",
        )
        .unique()
        .sort("article")
    )
    articles = df1["article"].to_list()
    products = df1["product"].to_list()

    st.markdown("###### Select a product for analysis.")
    col1, col2 = st.columns([2, 5], gap="large")
    with col1:
        article = st.selectbox(
            "Select an article from the list",
            articles,
            index=1,
        )

        pr_art = st.checkbox("Select product by product name", value=False)

        if not pr_art:
            product = df1.filter(pl.col("article") == article)["product"].head(1)[0]
            st.success(product)
        else:
            product = st.selectbox(
                "Select a product from the list",
                products,
                index=1,
            )
            article = df1.filter(pl.col("product") == product)["article"].head(1)[0]
            st.success(f"{article}")

        st.divider()
        date1 = st.date_input(
            "Select a date",
            df_de["date"].max(),
            key="date_range1",
        )
        st.divider()
        check = st.checkbox("Select prices with delivery", value=False)
        st.divider()

    df_de_prod = (
        df_de.filter(pl.col("article") == article)
        .join(
            hnp.select(
                pl.col("article", "year", "price", "subcategory", "family", "product")
            ),
            on=["article", "year"],
            how="left",
            # coalesce=True,
        )
        .with_columns(
            disc1=1 - pl.col("price") / pl.col("price_right"),
            disc2=1 - pl.col("price_delivery") / pl.col("price_right"),
        )
    )

    with col2:
        df_sel_date = df_de_prod.filter(pl.col("date") == date1)

        column = "disc2" if check else "disc1"
        column2 = "price_delivery" if check else "price"

        graph1 = create_chart(
            df_sel_date.sort(by=column, descending=True).head(12),
            f"Shops and prices for {product} with discounts from HNP",
        )
        st.plotly_chart(graph1, width='stretch')

    st.divider()

    col11, col12 = st.columns([1, 3], gap="large")

    with col11:
        st.markdown(
            "###### Preselected dates for analysis correspond to previous day, previous week, and previous month. You can use any dates for analysis by selecting the checkbox."
        )
        check_date = st.checkbox(
            "Select days for analysis", value=False, key="check_days"
        )
        date2 = st.date_input(
            "Select date 1", df_de_prod["date"].max(), key="date_range2"
        )
        date3 = st.date_input(
            "Select date 2", df_de_prod["date"].max(), key="date_range3"
        )
        date4 = st.date_input(
            "Select date 3", df_de_prod["date"].max(), key="date_range4"
        )
        date5 = st.date_input(
            "Select date 4", df_de_prod["date"].max(), key="date_range5"
        )

    with col12:
        if not check_date:
            previous_day = date1 - timedelta(
                days=1
            )  # Calculate the previous day, previous week, and previous month dates
            previous_week = date1 - timedelta(weeks=1)
            previous_month = date1 - timedelta(days=30)
        else:
            date1 = date2
            previous_day = date3
            previous_week = date4
            previous_month = date5
        # Filter the dataframe for the desired dates
        filtered_df = df_de_prod.filter(
            pl.col("date").is_in([date1, previous_day, previous_week, previous_month])
        ).sort("date", descending=False)

        pivot_df = filtered_df.pivot(
            values=column2, index="shop", columns="date", aggregate_function="min"
        )
        max_date = pivot_df.columns[-1]
        pivot_df = pivot_df.sort(by=max_date, descending=False, nulls_last=True)
        st.dataframe(pivot_df.head(10), width='stretch', hide_index=True)
