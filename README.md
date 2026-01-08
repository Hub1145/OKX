# OKX Trading Bot Dashboard

A real-time web application for monitoring and controlling an OKX trading bot with a custom limit order strategy.

## Features

### 🎯 Real-Time Dashboard
- **Console Output** - Live log stream showing signals, trades, and system events

### ⚙️ Bot Controls
- **Start/Stop** - Control bot execution with one click
- **Configuration Panel** - Modify all trading parameters through the UI
- **Light/Dark Mode** - Toggle between themes with persistent preference
- **Batch Modify TP/SL** - Manually adjust Take Profit and Stop Loss for all open positions.
- **Batch Cancel Orders** - Manually cancel all pending orders.
- **Emergency SL** - Immediately close all open positions at market price in critical situations. This now serves as the single mechanism for batch closing positions.

### 📊 Strategy & Parameters
- **Single Limit Order Strategy** - This bot employs a custom limit order strategy utilizing safety lines and price offsets for entry, Take Profit (TP), and Stop Loss (SL).
- **Dynamic TP/SL** - TP and SL levels are dynamically calculated based on entry price and configurable offsets.

## Quick Start

1.  **Download and Extract the Project**:
    *   Download the project as a ZIP file.
    *   Extract the contents to a local directory.
    *   Navigate to the project directory in your terminal.
2.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
3.  **Run the Application Locally**:
    ```bash
    python app.py
    ```
4.  **Access the Dashboard** - Open your web browser and navigate to `http://localhost:5000`.
5.  **Configure Settings** - Click the "Config" button to set your OKX API credentials and strategy parameters.
6.  **Start Trading** - Click "Start" to begin live trading.
7.  **Monitor Performance** - Watch real-time updates on the dashboard.

## Configuration

### General Settings
- **OKX API Key, Secret, Passphrase** - Your OKX API credentials.
- **Use Testnet** - Toggle for trading on OKX testnet.
- **Symbol** - Trading pair (e.g., ETH-USDT-SWAP)
- **Leverage** - The leverage to be used for trades.

### Strategy Parameters
- **Short Order Market Price Safety Line** - If the market price is below this, no short orders will be placed.
- **Long Order Market Price Safety Line** - If the market price is above this, no long orders will be placed.
- **Max Allowed Used (USDT)** - The maximum amount of USDT to be used for all orders combined, which is then divided by the Rate Divisor to determine the Max Amount per order.
- **Entry Price Offset** - Offset from current market price for the first limit order entry in a batch.
- **Batch Offset** - Additional offset applied to subsequent orders within a batch.
- **TP Price Offset** - Offset from entry price for Take Profit.
- **SL Price Offset** - Offset from entry price for Stop Loss.
- **Loop Time (seconds)** - Frequency of the main trading loop.
- **Rate Divisor** - Used in internal calculations to determine order size from Max Allowed Used.
- **Batch Size Per Loop** - Number of orders to attempt to place in a single trading cycle.
- **Min Order Amount** - Minimum quantity for an order.
- **Cancel Unfilled Order (seconds)** - Time after which an unfilled order is cancelled.
- **Cancel if TP price becomes unfavorable** - If enabled, pending entry orders will be cancelled if the calculated Take Profit price becomes less favorable than the current market price (e.g., TP price drops below market for a long position).
- **Cancel if Entry price becomes unfavorable** - If enabled, pending entry orders will be cancelled if the entry limit price becomes less favorable than the current market price (e.g., limit buy price is above market price).

## How It Works

1.  **Initialization** - On startup, the bot connects to OKX, fetches product info, and sets leverage.
2.  **Signal Detection** - The bot continuously monitors the market price and checks against defined safety lines for potential entry signals (long or short).
3.  **Batch Order Placement** - If a signal is detected, the bot initiates a batch of limit orders as defined by `Batch Size Per Loop`. Order sizing is now based on `Max Allowed Used` divided by `Rate Divisor` to determine the `Max Amount`. The first order uses the `Entry Price Offset`, and subsequent orders in the batch use an additional `Batch Offset` from the previous order's price.
4.  **Position Management** - Once an entry order is filled, corresponding Take Profit and Stop Loss algo orders are placed. **(Note: Real-time updates for positions and orders are no longer available via WebSocket due to public-only mode. Position status is checked via REST API.)**
5.  **Trade Management** - Open orders and positions are continuously monitored for TP/SL hits or cancellation conditions. Pending entry orders are also monitored for new cancellation conditions: if enabled, orders will be cancelled immediately if the TP price or Entry price becomes unfavorable relative to the current market price, overriding the time-based cancellation. **(Note: Real-time updates for orders are no longer available via WebSocket due to public-only mode. Order status is checked via REST API.)**
6.  **Batch Actions** - Manual controls are available on the dashboard to batch modify TP/SL, cancel all open orders, or trigger an "Emergency SL" which closes all open positions.

## Technology Stack

- **Backend**: Flask + Flask-SocketIO
- **Frontend**: Bootstrap 5 + Vanilla JavaScript
- **Data**: pandas, numpy for processing
- **API**: OKX REST and Public WebSocket API (Note: Private WebSocket channels for real-time order/position tracking are disabled.)

## Deployment to Railway.com

This section guides you through deploying your OKX Trading Bot Dashboard to Railway.com, a platform that simplifies application hosting.

1.  **Create a GitHub Repository**:
    *   Go to [GitHub](https://github.com/).
    *   Log in to your account.
    *   Click on the `+` sign in the top right corner and select `New repository`.
    *   Give your repository a name (e.g., `okx-trading-bot`), add a description, and choose whether it's public or private.
    *   **Do NOT initialize with a README, .gitignore, or license file** as you will be pushing your existing project.
    *   Click `Create repository`.
2.  **Initialize and Push Your Local Project to GitHub**:
    *   Open your terminal in the root directory of your extracted project.
    *   Initialize a new Git repository:
        ```bash
        git init
        ```
    *   Add your project files:
        ```bash
        git add .
        ```
    *   Commit your changes:
        ```bash
        git commit -m "Initial commit of OKX Trading Bot Dashboard"
        ```
    *   Connect your local repository to the GitHub repository you just created. You'll find the commands on your new GitHub repository page, typically:
        ```bash
        git remote add origin https://github.com/your-username/okx-trading-bot.git
        git branch -M main
        git push -u origin main
        ```
    *   Replace `your-username` and `okx-trading-bot` with your actual GitHub username and repository name.
3.  **Deploy to Railway.com**:
    *   Go to [Railway.com](https://railway.app/).
    *   Log in to your account.
    *   Click `New Project` -> `Deploy from GitHub Repo`.
    *   Connect your GitHub account to Railway (if you haven't already) and authorize Railway to access your repositories.
    *   Select the `okx-trading-bot` repository you just pushed.
    *   Railway will automatically detect your `Dockerfile` and `requirements.txt` and attempt to build and deploy your application.
    *   **Important**: You **must** add environment variables for your OKX API Key, Secret, and Passphrase (e.g., `OKX_API_KEY`, `OKX_API_SECRET`, `OKX_PASSPHRASE`) in Railway's project settings under "Variables" to match how your `bot_engine.py` expects them. For production, it's highly recommended to read these from environment variables rather than `config.json`.
    *   Once deployed, Railway will provide you with a public URL where your bot dashboard will be live.

## Important Notes

⚠️ **Risk Warning**: Trading carries significant risk. Always test with a demo account first.

🔑 **API Credentials**: You need valid OKX API Key, Secret, and Passphrase to use this bot.

🛑 **Configuration Changes**: Stop the bot before modifying configuration.

## Support

For issues or questions about OKX API, visit: https://www.okx.com/docs-v5/en/rest-api/

## License

This is an educational trading bot. Use at your own risk.
