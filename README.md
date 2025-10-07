# WMATA Performance Dashboard

Performance metrics dashboard for Washington DC Metro bus and rail lines.

## Setup

1. Install uv: `brew install uv`
2. Clone this repository
3. Get a WMATA API key from https://developer.wmata.com
4. Create a `.env` file in the project root with: `WMATA_API_KEY=your_key_here`
5. Install dependencies: `uv sync`
6. Run: `uv run python wmata_collector.py`

## Current Status

- [x] C51 bus route data collection
- [ ] Database storage
- [ ] Headway calculations  
- [ ] Web dashboard

## License

MIT
