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


def create_chart(df, title):  # Create a bar chart of the 'price' column
    smallest_price = df[column2].min()
    df["surplus"] = df[column2] - smallest_price
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
            df = pd.read_parquet(buffer, engine="pyarrow")
        return df

    df = load_data("./data/Ien.parquet")
    df_de = df[df["country"] == "de"]
    df_de = df_de.drop(columns=["country"])

    hnp = load_data("./data/hnp24.parquet")
    hnp.columns = hnp.columns.str.lower()

    df_de = df_de.merge(
        hnp[["article", "hnp", "subcategory", "family", "product"]],
        on="article",
        how="left",
    )

    df_de["disc1"] = 1 - df_de["price"] / df_de["hnp"]
    df_de["disc2"] = 1 - df_de["price_delivery"] / df_de["hnp"]

    st.markdown("###### Select a product for analysis.")
    col1, col2 = st.columns([2, 5], gap="large")
    with col1:
        product = st.selectbox(
            "Select a product from the list", df_de["product"].unique(), index=0
        )
        st.divider()
        date1 = st.date_input(
            "Select a date",
            df_de["date"].max(),
            key="date_range1",
        )
        st.divider()
        check = st.checkbox("Select prices with delivery", value=False)
        st.divider()

    with col2:
        df_de_prod = df_de[df_de["product"] == product].copy()
        df_de_prod["date"] = pd.to_datetime(df_de_prod["date"])
        date1 = pd.to_datetime(date1)
        df_sel_date = df_de_prod[df_de_prod["date"] == date1]

        column = "disc2" if check else "disc1"
        column2 = "price_delivery" if check else "price"

        graph1 = create_chart(
            df_sel_date.sort_values(by=column, ascending=False).head(12),
            f"Shops and prices for {product} with discounts from HNP",
        )
        st.plotly_chart(graph1, use_container_width=True)

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
            previous_day = pd.to_datetime(date1) - pd.DateOffset(
                days=1
            )  # Calculate the previous day, previous week, and previous month dates
            previous_week = pd.to_datetime(date1) - pd.DateOffset(weeks=1)
            previous_month = pd.to_datetime(date1) - pd.DateOffset(months=1)
        else:
            date1 = date2
            previous_day = date3
            previous_week = date4
            previous_month = date5
        # Filter the dataframe for the desired dates
        filtered_df = df_de_prod[
            df_de_prod["date"].isin(
                [date1, previous_day, previous_week, previous_month]
            )
        ].reset_index(drop=True)
        filtered_df["date"] = pd.to_datetime(filtered_df["date"]).dt.date
        filtered_df = filtered_df.sort_values(by="date", ascending=False)

        pivot_df = pd.pivot_table(
            filtered_df, values=column2, index="shop", columns="date"
        )
        max_date = pivot_df.columns.max()
        pivot_df = pivot_df.sort_values(by=max_date, ascending=True)
        st.dataframe(pivot_df.head(10), use_container_width=True)
