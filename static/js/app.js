const socket = io();

let currentConfig = null;
const configModal = new bootstrap.Modal(document.getElementById('configModal'));

document.addEventListener('DOMContentLoaded', () => {
    initializeTheme();
    loadConfig().then(() => {
        setupEventListeners();
        setupSocketListeners();
    });
});

function initializeTheme() {
    const savedTheme = localStorage.getItem('theme') || 'dark';
    document.body.setAttribute('data-theme', savedTheme);
    document.getElementById('themeToggle').checked = savedTheme === 'light';
    updateThemeIcon(savedTheme);
}

function updateThemeIcon(theme) {
    const icon = document.getElementById('themeIcon');
    icon.className = theme === 'light' ? 'bi bi-sun-fill' : 'bi bi-moon-stars';
}

function setupEventListeners() {
    document.getElementById('themeToggle').addEventListener('change', (e) => {
        const theme = e.target.checked ? 'light' : 'dark';
        document.body.setAttribute('data-theme', theme);
        localStorage.setItem('theme', theme);
        updateThemeIcon(theme);
    });

    document.getElementById('startBtn').addEventListener('click', () => {
        socket.emit('start_bot');
        document.getElementById('startBtn').disabled = true;
    });

    document.getElementById('stopBtn').addEventListener('click', () => {
        socket.emit('stop_bot');
        document.getElementById('stopBtn').disabled = true;
    });

    document.getElementById('configBtn').addEventListener('click', () => {
        loadConfigToModal();
        configModal.show();
    });

    document.getElementById('saveConfigBtn').addEventListener('click', () => {
        saveConfig();
    });

    document.getElementById('clearConsoleBtn').addEventListener('click', () => {
        socket.emit('clear_console');
        document.getElementById('consoleOutput').innerHTML = '<p class="text-muted">Console cleared</p>';
    });

    // Event listener for Emergency SL button
    document.getElementById('emergencySlBtn').addEventListener('click', () => {
        if (confirm('Are you sure you want to trigger an emergency Stop Loss? This will close all open positions at market price.')) {
            socket.emit('emergency_sl');
        }
    });

    // Event listener for Batch Modify TP/SL button
    document.getElementById('batchModifyTPSLBtn').addEventListener('click', () => {
        if (confirm('Are you sure you want to batch modify TP/SL for all open orders?')) {
            socket.emit('batch_modify_tpsl');
        }
    });

    // Event listener for Batch Cancel Orders button
    document.getElementById('batchCancelOrdersBtn').addEventListener('click', () => {
        if (confirm('Are you sure you want to batch cancel all open orders?')) {
            socket.emit('batch_cancel_orders');
        }
    });

}

function setupSocketListeners() {
    socket.on('connection_status', (data) => {
        console.log('Connected to server:', data);
    });

    socket.on('bot_status', (data) => {
        updateBotStatus(data.running);
    });

    socket.on('balance_update', (data) => {
        updateBalance(data.balance);
    });

    socket.on('trades_update', (data) => {
        updateOpenTrades(data.trades);
    });

    socket.on('position_update', (data) => {
        updatePositionDisplay(data);
    });

    socket.on('console_log', (data) => {
        addConsoleLog(data);
    });

    socket.on('console_cleared', () => {
        document.getElementById('consoleOutput').innerHTML = '<p class="text-muted">Console cleared</p>';
    });

    socket.on('price_update', (data) => {
    });

    socket.on('success', (data) => {
        showNotification(data.message, 'success');
    });

    socket.on('error', (data) => {
        showNotification(data.message, 'error');
    });

    socket.on('connect', () => {
        console.log('WebSocket connected');
        loadStatus();
    });

    socket.on('disconnect', () => {
        console.log('WebSocket disconnected');
    });
}

function updateBotStatus(running) {
    const statusBadge = document.getElementById('botStatus');
    const startBtn = document.getElementById('startBtn');
    const stopBtn = document.getElementById('stopBtn');

    if (running) {
        statusBadge.textContent = 'Running';
        statusBadge.className = 'badge status-badge running';
        startBtn.disabled = true;
        stopBtn.disabled = false;
    } else {
        statusBadge.textContent = 'Stopped';
        statusBadge.className = 'badge status-badge stopped';
        startBtn.disabled = false;
        stopBtn.disabled = true;
    }
}

function updateBalance(balance) {
    document.getElementById('balance').textContent = `$${balance.toFixed(2)}`;
}

function updatePositionDisplay(positionData) {
    const paramsContainer = document.getElementById('currentParams');
    const mlResultsContainer = document.getElementById('mlStrategyResults'); // Re-purpose this for current position details

    // Always display current config parameters in the "Current Parameters" section
    if (currentConfig) {
        let configHtml = `
            <div class="param-item">
                <span class="param-label">Symbol:</span>
                <span class="param-value">${currentConfig.symbol}</span>
            </div>
            <div class="param-item">
                <span class="param-label">Leverage:</span>
                <span class="param-value">${currentConfig.leverage}x</span>
            </div>
            <div class="param-item">
                <span class="param-label">Short Safety Line:</span>
                <span class="param-value">${currentConfig.short_safety_line_price}</span>
            </div>
            <div class="param-item">
                <span class="param-label">Long Safety Line:</span>
                <span class="param-value">${currentConfig.long_safety_line_price}</span>
            </div>
            <div class="param-item">
                <span class="param-label">Max Allowed Used:</span>
                <span class="param-value">${currentConfig.max_allowed_used}</span>
            </div>
            <div class="param-item">
                <span class="param-label">Max Amount:</span>
                <span class="param-value">${(currentConfig.max_allowed_used / currentConfig.rate_divisor).toFixed(2)}</span>
            </div>
            <div class="param-item">
                <span class="param-label">Entry Price Offset:</span>
                <span class="param-value">${currentConfig.entry_price_offset}</span>
            </div>
            <div class="param-item">
                <span class="param-label">Batch Offset:</span>
                <span class="param-value">${currentConfig.batch_offset}</span>
            </div>
            <div class="param-item">
                <span class="param-label">TP Price Offset:</span>
                <span class="param-value">${currentConfig.tp_price_offset}</span>
            </div>
            <div class="param-item">
                <span class="param-label">SL Price Offset:</span>
                <span class="param-value">${currentConfig.sl_price_offset}</span>
            </div>
            <div class="param-item">
                <span class="param-label">Loop Time:</span>
                <span class="param-value">${currentConfig.loop_time_seconds}s</span>
            </div>
            <div class="param-item">
                <span class="param-label">Rate Divisor:</span>
                <span class="param-value">${currentConfig.rate_divisor}</span>
            </div>
            <div class="param-item">
                <span class="param-label">Batch Size Per Loop:</span>
                <span class="param-value">${currentConfig.batch_size_per_loop}</span>
            </div>
            <div class="param-item">
                <span class="param-label">Min Order Amount:</span>
                <span class="param-value">${currentConfig.min_order_amount}</span>
            </div>
            <div class="param-item">
                <span class="param-label">Cancel Unfilled in:</span>
                <span class="param-value">${currentConfig.cancel_unfilled_seconds}s</span>
            </div>
        `;
        paramsContainer.innerHTML = configHtml;
    } else {
        paramsContainer.innerHTML = '<p class="text-muted">No parameters loaded yet.</p>';
    }


    if (!positionData || !positionData.in_position) {
        mlResultsContainer.innerHTML = '<p class="text-muted">No active position.</p>';
        return;
    }

    let positionHtml = `
        <div class="param-item">
            <span class="param-label">In Position:</span>
            <span class="param-value text-success">Yes</span>
        </div>
        <div class="param-item">
            <span class="param-label">Entry Price:</span>
            <span class="param-value">${positionData.position_entry_price.toFixed(4)}</span>
        </div>
        <div class="param-item">
            <span class="param-label">Quantity:</span>
            <span class="param-value">${positionData.position_qty.toFixed(4)}</span>
        </div>
        <div class="param-item">
            <span class="param-label">Current TP:</span>
            <span class="param-value text-success">${positionData.current_take_profit.toFixed(4)}</span>
        </div>
        <div class="param-item">
            <span class="param-label">Current SL:</span>
            <span class="param-value text-danger">${positionData.current_stop_loss.toFixed(4)}</span>
        </div>
    `;
    mlResultsContainer.innerHTML = positionHtml;
}

function updateOpenTrades(trades) {
    const tradesContainer = document.getElementById('openTrades');
    
    if (!trades || trades.length === 0) {
        tradesContainer.innerHTML = '<p class="text-muted">No open positions</p>';
        return;
    }

    tradesContainer.innerHTML = trades.map(trade => `
        <div class="trade-card ${trade.type.toLowerCase()}">
            <div class="trade-header">
                <span class="trade-type ${trade.type.toLowerCase()}">${trade.type}</span>
                <span class="trade-id">ID: ${trade.id}</span>
            </div>
            <div class="trade-details">
                <div class="trade-detail-item">
                    <span class="trade-detail-label">Entry:</span>
                    <span class="trade-detail-value">${trade.entry_spot_price !== null ? trade.entry_spot_price.toFixed(4) : 'N/A'}</span>
                </div>
                <div class="trade-detail-item">
                    <span class="param-label">Target Order:</span>
                    <span class="param-value">$${trade.stake !== null ? trade.stake.toFixed(2) : 'N/A'}</span>
                </div>
                <div class="trade-detail-item">
                    <span class="trade-detail-label">TP:</span>
                    <span class="trade-detail-value text-success">${trade.tp_price !== null ? trade.tp_price.toFixed(4) : 'N/A'}</span>
                </div>
                <div class="trade-detail-item">
                    <span class="trade-detail-label">SL:</span>
                    <span class="trade-detail-value text-danger">${trade.sl_price !== null ? trade.sl_price.toFixed(4) : 'N/A'}</span>
                </div>
            </div>
        </div>
    `).join('');
}

function addConsoleLog(log) {
    const consoleOutput = document.getElementById('consoleOutput');
    
    if (consoleOutput.querySelector('.text-muted')) {
        consoleOutput.innerHTML = '';
    }

    const logLine = document.createElement('div');
    logLine.className = `console-line ${log.level}`;
    logLine.innerHTML = `
        <span class="console-timestamp">[${log.timestamp}]</span>
        <span class="console-message">${escapeHtml(log.message)}</span>
    `;

    consoleOutput.appendChild(logLine);
    consoleOutput.scrollTop = consoleOutput.scrollHeight;

    if (consoleOutput.children.length > 500) {
        consoleOutput.removeChild(consoleOutput.firstChild);
    }
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

async function loadConfig() {
    try {
        const response = await fetch('/api/config');
        currentConfig = await response.json();
    } catch (error) {
        console.error('Error loading config:', error);
        showNotification('Failed to load configuration', 'error');
    }
}

async function loadStatus() {
    try {
        const response = await fetch('/api/status');
        const status = await response.json();
        
        updateBotStatus(status.running);
        updateBalance(status.balance);
        updateOpenTrades(status.open_trades);
        updatePositionDisplay(status); // Use the new function
    } catch (error) {
        console.error('Error loading status:', error);
    }
}

function loadConfigToModal() {
    if (!currentConfig) return;

    document.getElementById('okxApiKey').value = currentConfig.okx_api_key;
    document.getElementById('okxApiSecret').value = currentConfig.okx_api_secret;
    document.getElementById('okxPassphrase').value = currentConfig.okx_passphrase;
    document.getElementById('useTestnet').checked = currentConfig.use_testnet;
    document.getElementById('symbol').value = currentConfig.symbol;
    document.getElementById('shortSafetyLinePrice').value = currentConfig.short_safety_line_price;
    document.getElementById('longSafetyLinePrice').value = currentConfig.long_safety_line_price;
    document.getElementById('leverage').value = currentConfig.leverage;
    document.getElementById('maxAllowedUsed').value = currentConfig.max_allowed_used;
    document.getElementById('entryPriceOffset').value = currentConfig.entry_price_offset;
    document.getElementById('batchOffset').value = currentConfig.batch_offset;
    document.getElementById('tpPriceOffset').value = currentConfig.tp_price_offset;
    document.getElementById('slPriceOffset').value = currentConfig.sl_price_offset;
    document.getElementById('loopTimeSeconds').value = currentConfig.loop_time_seconds;
    document.getElementById('rateDivisor').value = currentConfig.rate_divisor;
    document.getElementById('batchSizePerLoop').value = currentConfig.batch_size_per_loop;
    document.getElementById('minOrderAmount').value = currentConfig.min_order_amount;
    document.getElementById('cancelUnfilledSeconds').value = currentConfig.cancel_unfilled_seconds;
    document.getElementById('cancelOnTpPriceBelowMarket').checked = currentConfig.cancel_on_tp_price_below_market;
    document.getElementById('cancelOnEntryPriceBelowMarket').checked = currentConfig.cancel_on_entry_price_below_market;
}

async function saveConfig() {
    const newConfig = {
        okx_api_key: document.getElementById('okxApiKey').value,
        okx_api_secret: document.getElementById('okxApiSecret').value,
        okx_passphrase: document.getElementById('okxPassphrase').value,
        use_testnet: document.getElementById('useTestnet').checked,
        symbol: document.getElementById('symbol').value,
        short_safety_line_price: parseFloat(document.getElementById('shortSafetyLinePrice').value),
        long_safety_line_price: parseFloat(document.getElementById('longSafetyLinePrice').value),
        leverage: parseInt(document.getElementById('leverage').value),
        max_allowed_used: parseFloat(document.getElementById('maxAllowedUsed').value),
        entry_price_offset: parseFloat(document.getElementById('entryPriceOffset').value),
        batch_offset: parseFloat(document.getElementById('batchOffset').value),
        tp_price_offset: parseFloat(document.getElementById('tpPriceOffset').value),
        sl_price_offset: parseFloat(document.getElementById('slPriceOffset').value),
        loop_time_seconds: parseInt(document.getElementById('loopTimeSeconds').value),
        rate_divisor: parseInt(document.getElementById('rateDivisor').value),
        batch_size_per_loop: parseInt(document.getElementById('batchSizePerLoop').value),
        min_order_amount: parseFloat(document.getElementById('minOrderAmount').value),
        cancel_unfilled_seconds: parseInt(document.getElementById('cancelUnfilledSeconds').value),
        cancel_on_tp_price_below_market: document.getElementById('cancelOnTpPriceBelowMarket').checked,
        cancel_on_entry_price_below_market: document.getElementById('cancelOnEntryPriceBelowMarket').checked,
    };

    try {
        const response = await fetch('/api/config', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(newConfig),
        });

        const result = await response.json();

        if (result.success) {
            currentConfig = newConfig;
            configModal.hide();
            showNotification('Configuration saved successfully', 'success');
        } else {
            showNotification(result.message, 'error');
        }
    } catch (error) {
        console.error('Error saving config:', error);
        showNotification('Failed to save configuration', 'error');
    }
}

function showNotification(message, type) {
    const alertDiv = document.createElement('div');
    alertDiv.className = `alert alert-${type === 'success' ? 'success' : 'danger'} alert-dismissible fade show position-fixed top-0 start-50 translate-middle-x mt-3`;
    alertDiv.style.zIndex = '9999';
    alertDiv.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;

    document.body.appendChild(alertDiv);

    setTimeout(() => {
        alertDiv.remove();
    }, 5000);
}
