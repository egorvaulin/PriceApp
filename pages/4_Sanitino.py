import streamlit as st
import pandas as pd
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
    page_title="E-trader analysis",
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

col1, col2, col3 = st.columns([4, 1, 1])
with col1:
    st.markdown('## Sanitino analysis')
with col2:
    czk = st.number_input('CZK rate:', value=25.5)
with col3:
    ron = st.number_input('RON rate:', value=4.98)
st.divider()

@st.cache_data
def load_data(path):
    # df = pd.read_csv(path, parse_dates=['date'], dtype={6: str}) # Import the data from csv
    with open(path, 'rb') as f:
        encrypted_data = f.read()
        buffer = io.BytesIO(decrypt_data(encrypted_data, key))
        df = pd.read_parquet(buffer, engine='pyarrow')
    return df

def custom_metric(label, value):
    st.markdown(f"""
        <div style="border:1px solid #343499; border-left:8px solid #343499; border-radius:2px; padding:10px; margin:5px; text-align:center; height:180px; overflow:auto;">
            <h5>{label}</h5>
            <h2>{value}</h2>
        </div>
        """, unsafe_allow_html=True)
    
def calculate_price(row, czk, ron):
    if row['country'] == 'cz':
        return row['price'] / czk
    elif row['country'] == 'ro':
        return row['price'] / ron
    else:
        return row['price']

df = load_data("./data/Sen.parquet")
vat = pd.DataFrame({'country': ['de', 'be', 'cz', 'fr', 'it', 'sk', 'ro', 'es'],
                    'vat': [0.19, 0.21, 0.21, 0.2, 0.22, 0.20, 0.19, 0.21]})
vat.set_index('country', inplace=True)
# hnp = load_data_xls("./data/Price List Germany 2024.xlsx")
hnp = load_data('./data/hnp24.parquet')
hnp.columns = hnp.columns.str.lower()

df_s = df.merge(hnp[['article', 'hnp', 'subcategory', 'family', 'product']], on='article', how='left')
df_s['date'] = pd.to_datetime(df_s['date']).dt.date

subcat = df_s['subcategory'].unique()

col1, col2, col3, col4 = st.columns([1, 2, 1, 1])
with col1:
    subcategory = st.selectbox('Select a subcategory', subcat, index=6)
    df_s = df_s[df_s['subcategory'] == subcategory]
with col2:
    prod = df_s['product'].unique()
    product = st.selectbox('Select a product', prod, index=0)
with col3:
    fam = df_s['family'].unique()
    family = st.selectbox('Select a family', fam, index=0)
    family_check = st.checkbox('Use family for calculation', value=False)
with col4:
    last_day = df_s['date'].max()
    previous_day = (df_s['date'].max() - pd.DateOffset(days=1)).date()
    previous_week = (df_s['date'].max() - pd.DateOffset(weeks=1)).date()
    previous_month = (df_s['date'].max() - pd.DateOffset(months=1)).date()
    date = st.date_input('Select a date in a format YYYY/MM/DD', df_s['date'].max() - pd.DateOffset(days=1), 
                         min_value=df_s['date'].min(), max_value=last_day)

df_sp = df_s[df_s['product'] == product]

if family_check:
    df_sp = df_s.groupby(['family', 'country', 'date']).agg({'price': 'mean'}).reset_index()
    df_sp = df_sp[df_sp['family'] == family]
else:
    pass

df_sp.loc[:,'price_eur'] = df_sp.apply(calculate_price, args=(czk, ron), axis=1).round(2)

df_sp = df_sp.merge(vat, left_on='country', right_index=True, how='left')
df_sp['price_net'] = (df_sp['price_eur'] / (1 + df_sp['vat'])).round(2)
df_sp.drop(columns=['price','vat'], inplace=True)
df_spp = df_sp[df_sp['date'].isin([last_day, previous_day, previous_week, previous_month])].reset_index(drop=True)

fig = go.Figure()

colors = ['#7d98a1', '#343499', '#9fb3ba', '#7676bb']
for day, color in zip(df_spp['date'].unique(), colors):
    df_date = df_spp[df_spp['date'] == day]
    formatted_date = day.strftime("%d %b")  # Format date as "day month"
    sorted_countries = sorted(df_date['country'])  # Sort countries in alphabetical order
    fig.add_trace(go.Bar(
    x=sorted_countries, 
    y=df_date['price_net'], 
    name=f'Net price EUR on {formatted_date}', 
    marker_color=color,
    text=df_date['price_net'].round(1).astype(str),  # Add values as text
    textposition='auto',  # Position text inside the bars
    textfont=dict(color='white')  # Change text color to white
))

fig.update_layout(
    title='Price by Country',
    xaxis_title=None,
    yaxis_title='Price EUR',
    xaxis=dict(  # Modify x-axis labels
        tickfont=dict(
            size=16,  # Increase font size
            color='#343499'
        ),
    ),
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
st.divider()

df_spd = df_sp[df_sp['date'] >= date]
countries = df_spd['country'].unique()
col11, col12 = st.columns([1,7])
with col11:
    try:
        countries_selected = st.multiselect('Select countries', countries, default=['cz', 'de', 'es', 'sk'])
    except:
        pass
with col12:
    colors = ['#7d98a1', '#343499','#fbe059', '#585858', '#86d277', '#9fb3ba', '#7676bb', '#818181']
    fig_line = go.Figure()
    try:
        for country, color in zip(countries_selected, colors[:len(countries_selected)]):
            df_country = df_spd[df_spd['country'] == country]
            sorted_dates = sorted(df_country['date'])  # Sort dates in ascending order
            fig_line.add_trace(go.Scatter(x=sorted_dates, y=df_country['price_net'], name=country, marker_color=color))

        fig_line.update_layout(
            title='Price by Country over Time',
            xaxis_title=None,
            yaxis_title='Price EUR',
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
    except:
        pass
    st.plotly_chart(fig_line, use_container_width=True)
