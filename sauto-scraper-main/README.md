# sauto-scraper

## Description

This is a Scrapy-based web scraper that extracts car listings from the [sauto.cz](https://www.sauto.cz/) API.
The scraper queries the API using configurable parameters stored in `params.json` and saves the output in JSON or CSV format for further analysis.

## Features

- Configurable search parameters via `params.json`
- Supports filtering by manufacturer + model
- Supports filtering by seller type (private / dealer)
- Supports price range filtering
- Logs all scraped URLs to `sauto_spider.log`
- Outputs data in JSON / CSV / JSONL format
- Uses the sauto.cz API endpoint

## Installation

1. Clone the repository

```bash
git clone https://github.com/karlosmatos/sauto-scraper.git
cd sauto-scraper
```

2. Create a virtual environment (recommended)

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install the requirements

```bash
pip install -r requirements.txt
```

## Configuration

Modify the `params.json` file to customize your search parameters:

```json
{
  "limit": "1000",
  "offset": "0",

  "category_id": "838",

  "manufacturer_seo_name": "volkswagen",
  "model_seo_name": "golf",

  "condition_seo": "nove,ojete,predvadeci",
  "operating_lease": "false",
  "seller_type": "soukromy",

  "price_from": "0",
  "price_to": "300000"
}
```

---

## Parameter Descriptions

### Pagination / output size
- `limit`: Maximum number of results per request (usually max 1000)
- `offset`: Starting offset for pagination (default: `"0"`)

### Vehicle category
- `category_id`: Vehicle category filter
  - `838` = Osobní (personal cars)
  - Other categories exist (e.g. užitkové), but this scraper example focuses on personal cars

### Manufacturer + model filtering
- `manufacturer_seo_name`: Manufacturer filter (SEO name)
  - Example: `"volkswagen"`, `"audi"`, `"bmw"`, `"skoda"`
- `model_seo_name`: Model filter (SEO name)
  - Example: `"golf"`, `"passat"`, `"a4"`, `"octavia"`

### Condition filter
- `condition_seo`: Filter by car condition (comma-separated)
  - `nove` = new
  - `ojete` = used
  - `predvadeci` = demo

### Seller filter
- `seller_type`: Filter by seller type
  - `"soukromy"` = private seller
  - `"bazar"` = dealer / car lot

### Lease filter
- `operating_lease`: Filter operating lease vehicles (`"true"` / `"false"`)

### Price filter
- `price_from`: Minimum price in CZK
- `price_to`: Maximum price in CZK

---

## Example Configurations

### Volkswagen Golf (private sellers only)
```json
{
  "limit": "1000",
  "offset": "0",
  "category_id": "838",
  "manufacturer_seo_name": "volkswagen",
  "model_seo_name": "golf",
  "seller_type": "soukromy",
  "condition_seo": "nove,ojete,predvadeci",
  "operating_lease": "false",
  "price_from": "0",
  "price_to": "300000"
}
```

### Volkswagen Golf (dealers / bazaars only)
```json
{
  "limit": "1000",
  "offset": "0",
  "category_id": "838",
  "manufacturer_seo_name": "volkswagen",
  "model_seo_name": "golf",
  "seller_type": "bazar",
  "condition_seo": "nove,ojete,predvadeci",
  "operating_lease": "false",
  "price_from": "0",
  "price_to": "300000"
}
```

### Audi A4 (any seller type)
```json
{
  "limit": "1000",
  "offset": "0",
  "category_id": "838",
  "manufacturer_seo_name": "audi",
  "model_seo_name": "a4",
  "condition_seo": "ojete",
  "operating_lease": "false",
  "price_from": "0",
  "price_to": "500000"
}
```

---

## Usage

### Basic Usage

Run the scraper to output JSON:

```bash
python -m scrapy crawl sauto -O data/sauto.json
```

### Output Formats

```bash
# JSON format
python -m scrapy crawl sauto -O data/sauto.json

# CSV format
python -m scrapy crawl sauto -O data/sauto.csv

# JSON Lines format
python -m scrapy crawl sauto -O data/sauto.jl
```

---

## Logs

The scraper logs all scraped URLs with timestamps to `sauto_spider.log` for debugging and monitoring purposes.

---

## Project Structure

```
sauto-scraper/
├── sauto/
│   ├── spiders/
│   │   └── sauto_spider.py    # Main spider implementation
│   ├── items.py                # Item definitions
│   ├── pipelines.py           # Data processing pipelines
│   └── settings.py            # Scrapy settings
├── data/                       # Output directory
├── params.json                 # Search parameters configuration
├── requirements.txt            # Python dependencies
└── scrapy.cfg                  # Scrapy configuration
```

---

## How It Works

1. The spider reads parameters from `params.json`
2. It queries the sauto.cz API endpoint:  
   `https://www.sauto.cz/api/v1/items/search?`
3. Results are parsed from JSON responses and saved to the output file
4. All URLs are logged to `sauto_spider.log` with timestamps

---

## License

[MIT](https://choosealicense.com/licenses/mit/)
