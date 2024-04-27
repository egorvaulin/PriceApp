import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import toml
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import pyarrow.parquet as pq
import io

# config = toml.load("./.streamlit/secrets.toml")
# key = config["secrets"]["data_key"].encode('utf-8')
key = st.secrets["data_key"]

def decrypt_data(data, key):
    cipher = AES.new(key, AES.MODE_CBC, iv=data[:16])
    pt = unpad(cipher.decrypt(data[16:]), AES.block_size)
    return pt

# Page configuration
st.set_page_config(
    page_title="Product analysis",
    layout="wide",
    initial_sidebar_state="expanded")

# Change the font of the entire app
def set_font(font):
    st.markdown(f"""
                <style>
                body {{font-family: {font};}}
                </style>
                """, unsafe_allow_html=True)

set_font('Arial')

# --- HIDE STREAMLIT STYLE ---
hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)

st.markdown('## Product analysis')
st.divider()

@st.cache_data
def load_data(path):
    # df = pd.read_csv(path, parse_dates=['date'], dtype={6: str}) # Import the data from csv
    with open(path, 'rb') as f:
        encrypted_data = f.read()
        buffer = io.BytesIO(decrypt_data(encrypted_data, key))
        df = pd.read_parquet(buffer, engine='pyarrow')
    return df

df = load_data("./data/Ien.parquet")
df_de = df[df['country'] == 'de']

hnp = load_data("./data/hnp24.parquet")
hnp.columns = hnp.columns.str.lower()

df_de = df_de.merge(hnp[['article', 'hnp', 'subcategory', 'family', 'product']], on='article', how='left')
df_de.drop(columns=['country'], inplace=True)
df_de['disc1'] = 1 - df_de['price']/df_de['hnp']
df_de['disc2'] = 1 - df_de['price_delivery']/df_de['hnp']

st.markdown('###### Select a product either by choosing a subcategory, family, and product or by selecting a product directly. If you choose directly do not forget to mark checkbox.')
col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    subcategory = st.selectbox('Select a subcategory', df_de['subcategory'].unique(), index=0)
    df_de_sub = df_de[df_de['subcategory'] == subcategory]
with col2:
    family = st.selectbox('Select a family from the subcategory', df_de_sub['family'].unique(), index=0)
    df_de_fam = df_de_sub[df_de_sub['family'] == family]
with col3:
    product = st.selectbox('Select a product from the family', df_de_fam['product'].unique(), index=0)
with col4:
    product1 = st.selectbox('Select a product', df_de['product'].unique(), index=1)
    check = st.checkbox('Select this product', value=False)
with col5:
    discount = st.slider('Discount', 40, 60, step=1, value=50, format="%d%%", key='discount_slider', help='Select the discount percentage')
    check2 = st.checkbox('Select this price with delivery', value=False)

# Select the data based on the user input
if check:
    prod = product1
else:
    prod = product

df_de_prod = df_de[df_de['product'] == prod]
last_date = df_de_prod['date'].max()
df_last_day = df_de_prod[df_de_prod['date'] == last_date]
# Split the data frame into two based on the discount threshold
column = 'disc2' if check2 else 'disc1'
column2 = 'price_delivery' if check2 else 'price'
df_below_threshold = df_last_day[df_last_day[column] <= discount/100]
df_above_threshold = df_last_day[df_last_day[column] > discount/100]

def create_chart(df, title):  # Create a bar chart of the 'price' column
    chart = go.Figure(data=[go.Bar(x=df['shop'], y=df[column2], name=column2, marker_color='#d1d1e8')])

    chart.add_trace(go.Scatter(       # Add 'price' values at the bottom of the bars
        x=df['shop'], 
        y=[0.1]*len(df),  # Set 'y' to a small number
        mode='text',  # Set the mode to 'text'
        text=[f'{val:.1f}' for val in df[column2]],  # Set the 'text' to the 'price' values
        textposition='top center',  # Position the text at the top of the 'y' position
        textfont=dict(family='Arial', size=12, color='black'),
        showlegend=False  # Do not show this trace in the legend
    ))

    chart.add_trace(go.Scatter(    # Add 'disc1' to the y2 axis with only markers
        x=df['shop'], 
        y=df[column], 
        name=column, 
        yaxis='y2', 
        mode='markers+text',  # Add 'text' to the mode
        marker=dict(color='#343499', size=10), # Increase the size of the markers
        text=[f'{val:.1%}' for val in df[column]],  # Format 'disc1' as a percentage with 1 decimal place
        textposition='top center',  # Position the text above the markers
        textfont=dict(family='Arial', size=12, color='black')
    ))

    chart.update_layout(   # Update the layout to include the secondary y-axis and remove all gridlines
        title_text=title,
        yaxis=dict(title=column2, showgrid=False),
        yaxis2=dict(title='', overlaying='y', side='right', showticklabels=False, ticks='', showgrid=False, range=[0, max(df[column])+0.05]),  # Adjust the range for 'y2'
        xaxis=dict(showgrid=False),
        showlegend=False  # Remove the legend
    )
    return chart

# Check if the data frames are empty
if not df_below_threshold.empty:
    graph2 = create_chart(df_below_threshold, f'Shops and prices for {prod} below {discount/100:.0%} discounts from HNP')
else:
    st.warning(f'No shops found for {prod} below {discount/100:.0%} discounts from HNP')
    graph2 = go.Figure()

if not df_above_threshold.empty:
    graph1 = create_chart(df_above_threshold, f'Shops and prices for {prod} above {discount/100:.0%} discounts from HNP')
else:
    st.warning(f'No shops found for {prod} above {discount/100:.0%} discounts from HNP')
    graph1 = go.Figure()

col21, col22, col23, col24 = st.columns([5, 1, 1, 5])
with col21:
    st.plotly_chart(graph1, use_container_width=True)
with col22:
    st.empty()
with col23:
    st.markdown('<div style="border-left:2px solid gray; height:450px; margin: auto;"></div>', unsafe_allow_html=True)
with col24:
    st.plotly_chart(graph2, use_container_width=True)

st.divider()

col11, col12 = st.columns([1, 2])
with col12:
    fig = go.Figure()

    min_price = df_de_prod.groupby('date')[column2].min()  # Calculate minimum and maximum prices per date
    mean_price = df_de_prod.groupby('date')[column2].mean()
    max_price = df_de_prod.groupby('date')[column2].max()

    fig.add_trace(go.Scatter(                   # Add minimum price line
        x=min_price.index,
        y=min_price,
        mode='lines',
        name='Minimum Price',
        line=dict(color='#818181')
    ))

    fig.add_trace(go.Scatter(            # Add average price line
        x=mean_price.index,
        y=mean_price,
        mode='lines',
        name='Mean Price',
        line=dict(color='#86d277', dash='dash')
    ))

    fig.add_trace(go.Scatter(       # Add maximum price line
        x=max_price.index,
        y=max_price,
        mode='lines',
        name='Maximum Price',
        line=dict(color='#7676bb')
    ))

    fig.update_layout(
        title=f'Minimum, Average and Maximum Price for {prod}',
        xaxis_title=None,
        yaxis=dict(title='Price'),
        plot_bgcolor='white',
        paper_bgcolor='white',
        legend=dict(
            yanchor="top",
            y=-0.2,  # Position legend below the graph
            xanchor="center",
            x=0.5,
            orientation="h",  # Horizontal orientation
            font=dict(
                size=16,  # Increase font size
                color='#343499'
            ),
        )
    )
    st.plotly_chart(fig, use_container_width=True)

with col11:
    previous_day = pd.to_datetime(last_date) - pd.DateOffset(days=1) # Calculate the previous day, previous week, and previous month dates
    previous_week = pd.to_datetime(last_date) - pd.DateOffset(weeks=1)
    previous_month = pd.to_datetime(last_date) - pd.DateOffset(months=1)
    # Filter the dataframe for the desired dates
    filtered_df = df_de_prod[df_de_prod['date'].isin([last_date, previous_day, previous_week, previous_month])].reset_index(drop=True)

    cols =  ['month ago', 'week ago', 'day ago', 'last day']
    pivot_df = pd.pivot_table(filtered_df, values=column2, index='shop', columns='date')
    pivot_df = pivot_df.rename(dict(zip(pivot_df.columns, cols)), axis=1)
    pivot_df = pivot_df.iloc[:, ::-1].dropna(subset=['last day']).sort_values(by='last day', ascending=True)
    pivot_df.iloc[:, 1:] = pivot_df.iloc[:, 0:1].values - pivot_df.iloc[:, 1:]
    st.write(pivot_df.head(10))
