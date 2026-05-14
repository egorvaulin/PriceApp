# PriceApp

A Streamlit dashboard for monitoring e-commerce product prices across Germany, France, and the UK.

## Features

- Price trend charts per country (DE, FR, UK)
- Per-product price analysis with competitor comparison
- Shop-level analysis and minimum price tracking
- Hybrid pricing metrics and Sanitino retailer analysis
- Data updated weekly via external scraping pipeline

## Setup

### Prerequisites

- Python 3.10+
- pip

### Installation

```bash
pip install -r requirements.txt
```

### Secrets

Create `.streamlit/secrets.toml` with your encryption key:

```toml
[secrets]
data_key = "your-32-char-encryption-key-here"
```

On Streamlit Cloud, add `secrets.data_key` via the app's Secrets settings instead.

### Running locally

```bash
streamlit run Intro.py
```

## Project structure

```
PriceApp/
├── Intro.py                        # Home page / summary dashboard
├── middleware.py                   # Authentication
├── requirements.txt
├── pages/
│   ├── 1_Price_Development_DE.py   # Germany price trends
│   ├── 2_Price_Development_FR.py   # France price trends
│   ├── 3_Price_Development_UK.py   # UK price trends
│   ├── 4_Analysis_per_product_DE.py
│   ├── 5_Analysis_per_product_FR.py
│   ├── 6_Analysis_per_shop.py
│   ├── 7_Analysis_min_price.py
│   ├── 8_Sanitino.py
│   └── 9_Hybrid.py
└── data/                           # Encrypted parquet files (not in git)
```

## Data

Price data is stored as AES-encrypted parquet files in `data/`. The main files are:

| File | Contents |
|------|----------|
| `Ien.parquet` | Main price dataset (DE, FR, UK) |
| `Sen.parquet` | Sanitino retailer prices |
| `tlp.parquet` | Target/list prices |
| `an.parquet` | Anchor prices |
| `Logs.parquet` | User credentials |
