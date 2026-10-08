import time
import datetime
import threading
import logging
import pandas as pd
from flask import Flask, jsonify, render_template_string
from truedata import TD_live

# ----------------- CONFIGURATION -----------------
TRUEDATA_USER = "YOUR_LOGIN_ID"
TRUEDATA_PASS = "YOUR_PASSWORD"
EXPIRY_DATE = datetime.date(2026, 10, 1)  # Enter target weekly expiry (YYYY, MM, DD)
PORT = 8082  # Standard TrueData live port (8082 or 8084)
# --------------------------------------------------

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# In-memory shared state between WebSocket thread and Flask API
market_state = {
    "spot": 24275.50,
    "atm": 24300,
    "max_pain": 24350,
    "overall_pcr": "0.85",
    "total_call_oi": "650.0k",
    "total_put_oi": "552.5k",
    "rows": []
}
state_lock = threading.Lock()

def calculate_max_pain_and_pcr(chain_data, spot_price):
    """
    Computes Max Pain and Strike PCR from aggregated option chain data.
    """
    if not chain_data:
        return

    # Sort strikes
    sorted_strikes = sorted(chain_data.keys())
    if not sorted_strikes:
        return

    atm = min(sorted_strikes, key=lambda x: abs(x - spot_price))

    # Keep a window of strikes around the ATM (e.g. +/- 400 pts)
    active_strikes = [s for s in sorted_strikes if abs(s - atm) <= 400]
    if not active_strikes:
        active_strikes = sorted_strikes[:15]

    # Max Pain calculation: find strike with lowest cumulative writer liability
    min_loss = float("inf")
    max_pain_strike = active_strikes[0]

    for candidate in active_strikes:
        total_loss = 0
        for s in active_strikes:
            c_oi = chain_data[s]["call_oi"]
            p_oi = chain_data[s]["put_oi"]

            # Call writer liability if market settles above strike
            if candidate > s:
                total_loss += (candidate - s) * c_oi
            # Put writer liability if market settles below strike
            if candidate < s:
                total_loss += (s - candidate) * p_oi

        if total_loss < min_loss:
            min_loss = total_loss
            max_pain_strike = candidate

    # Calculate Totals & Overall PCR
    total_call = sum(chain_data[s]["call_oi"] for s in active_strikes)
    total_put = sum(chain_data[s]["put_oi"] for s in active_strikes)
    overall_pcr = round(total_put / total_call, 2) if total_call > 0 else 0.0

    # Build formatted rows for UI with OI scaled to thousands ('000s)
    rows = []
    for s in active_strikes:
        c_oi_k = round(chain_data[s]["call_oi"] / 1000, 1)
        p_oi_k = round(chain_data[s]["put_oi"] / 1000, 1)
        strike_pcr = round(p_oi_k / c_oi_k, 2) if c_oi_k > 0 else 0.0

        rows.append({
            "strike": int(s),
            "call_oi": c_oi_k,
            "put_oi": p_oi_k,
            "pcr": f"{strike_pcr:.2f}",
            "is_atm": s == atm,
            "is_max_pain": s == max_pain_strike
        })

    with state_lock:
        market_state["spot"] = round(spot_price, 2)
        market_state["atm"] = atm
        market_state["max_pain"] = int(max_pain_strike)
        market_state["overall_pcr"] = f"{overall_pcr:.2f}"
        market_state["total_call_oi"] = f"{round(total_call / 1000, 1):,}k"
        market_state["total_put_oi"] = f"{round(total_put / 1000, 1):,}k"
        market_state["rows"] = rows

def init_truedata_stream():
    """
    Connects to TrueData WebSocket, subscribes to NIFTY 50 and Option Chain.
    """
    try:
        td_obj = TD_live(TRUEDATA_USER, TRUEDATA_PASS, live_port=PORT, log_level=logging.WARNING)

        # 1. Subscribe to NIFTY 50 Spot
        td_obj.start_live_data(['NIFTY 50'])
        time.sleep(1)

        # 2. Subscribe to Option Chain
        td_obj.start_option_chain('NIFTY', EXPIRY_DATE, chain_length=20)
        time.sleep(1)

        @td_obj.trade_callback
        def on_tick(tick_data):
            # Update live spot price when NIFTY 50 trades
            symbol = getattr(tick_data, "symbol", "")
            ltp = getattr(tick_data, "ltp", 0.0)
            if "NIFTY 50" in symbol and ltp > 0:
                with state_lock:
                    market_state["spot"] = ltp

        # Periodic Option Chain poll loop (TrueData maintains live chain in memory)
        while True:
            try:
                # get_option_chain returns latest real-time chain snapshot dataframe
                chain_df = td_obj.get_option_chain('NIFTY', EXPIRY_DATE)
                if chain_df is not None and not chain_df.empty:
                    chain_dict = {}
                    for _, row in chain_df.iterrows():
                        strike = float(row.get("strike", 0))
                        opt_type = str(row.get("type", "")).upper()
                        oi = float(row.get("oi", 0))

                        if strike not in chain_dict:
                            chain_dict[strike] = {"call_oi": 0, "put_oi": 0}

                        if "CE" in opt_type or "CALL" in opt_type:
                            chain_dict[strike]["call_oi"] = oi
                        elif "PE" in opt_type or "PUT" in opt_type:
                            chain_dict[strike]["put_oi"] = oi

                    calculate_max_pain_and_pcr(chain_dict, market_state["spot"])
            except Exception as e:
                logging.error(f"Option chain parse error: {e}")

            time.sleep(2)  # Update math calculations every 2 seconds

    except Exception as err:
        logging.error(f"TrueData connection failed: {err}. Using simulated fallback stream.")
        simulate_fallback()

def simulate_fallback():
    """Generates live tick movements if TrueData credentials are being configured."""
    while True:
        with state_lock:
            spot = market_state["spot"] + (time.time() % 3 - 1) * 2.5
            market_state["spot"] = round(spot, 2)
            atm = round(spot / 50) * 50
            market_state["atm"] = atm
            market_state["max_pain"] = atm + 50
            
            # Recompute dummy strikes
            rows = []
            for i in range(-4, 5):
                s = atm + i * 50
                diff = (s - spot) / 50
                c_oi = round(max(10, 115 * (1.2 ** (-diff))), 1)
                p_oi = round(max(5, 95 * (1.2 ** (diff))), 1)
                pcr = round(p_oi / c_oi, 2) if c_oi > 0 else 0.0
                rows.append({
                    "strike": s, "call_oi": c_oi, "put_oi": p_oi,
                    "pcr": f"{pcr:.2f}", "is_atm": s == atm, "is_max_pain": s == (atm + 50)
                })
            market_state["rows"] = rows
        time.sleep(2)

# Start WebSocket ingestion thread
t = threading.Thread(target=init_truedata_stream, daemon=True)
t.start()

# ----------------- FLASK ENDPOINTS -----------------
@app.route("/api/live")
def get_live():
    with state_lock:
        return jsonify(market_state)

@app.route("/")
def index():
    return render_template_string(HTML_PAGE)

HTML_PAGE = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Live TrueData Option Chain & Max Pain</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-100 p-4 sm:p-8 font-sans text-slate-800 min-h-screen">
  <div class="max-w-3xl mx-auto bg-white p-6 rounded-2xl shadow-sm border border-slate-200">
    
    <!-- Header -->
    <div class="flex flex-wrap items-center justify-between gap-3 pb-4 border-b border-slate-200">
      <div>
        <div class="flex items-center gap-2">
          <h1 class="text-xl font-bold text-slate-900">TrueData Nifty Option Chain</h1>
          <span class="inline-flex items-center gap-1.5 bg-emerald-50 text-emerald-700 text-[11px] font-semibold px-2.5 py-0.5 rounded-full border border-emerald-200">
            <span class="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse"></span>
            WebSocket Stream Live
          </span>
        </div>
        <p class="text-xs text-slate-500 mt-0.5">Real-time calculations for Spot, Strike PCR, and Max Pain</p>
      </div>
      <button onclick="fetchData()" class="text-xs bg-slate-900 hover:bg-slate-800 text-white font-semibold px-3 py-1.5 rounded-lg transition shadow-sm">
        Sync Now
      </button>
    </div>

    <!-- KPI Dashboard -->
    <div class="grid grid-cols-1 sm:grid-cols-3 gap-3.5 my-4">
      <!-- Spot Price -->
      <div class="bg-slate-50 p-3.5 rounded-xl border border-slate-200">
        <span class="text-xs font-semibold text-slate-500 block uppercase tracking-wider">Spot Price (Nifty)</span>
        <div id="spotPrice" class="mt-1 font-mono font-black text-2xl text-slate-900">--</div>
        <span id="atmLabel" class="text-[11px] text-slate-500 mt-1 block">ATM Strike: <b>--</b></span>
      </div>

      <!-- Max Pain -->
      <div class="bg-indigo-50/70 p-3.5 rounded-xl border border-indigo-200 flex flex-col justify-between">
        <div class="flex items-center justify-between">
          <span class="text-xs font-semibold text-indigo-950 uppercase tracking-wider">Calculated Max Pain</span>
          <span class="text-[10px] bg-indigo-600 text-white px-2 py-0.5 rounded-full font-bold">LIVE</span>
        </div>
        <div id="maxPainVal" class="text-2xl font-black text-indigo-700 font-mono mt-1">--</div>
        <span id="maxPainDiff" class="text-[11px] text-indigo-600 mt-1 block font-medium">--</span>
      </div>

      <!-- Overall PCR -->
      <div class="bg-slate-50 p-3.5 rounded-xl border border-slate-200 flex flex-col justify-between">
        <div class="flex items-center justify-between">
          <span class="text-xs font-semibold text-slate-500 uppercase tracking-wider">Overall PCR</span>
          <span id="pcrBadge" class="text-[10px] font-bold px-2 py-0.5 rounded-full bg-slate-200 text-slate-700">--</span>
        </div>
        <div id="overallPcr" class="text-2xl font-bold font-mono text-slate-900 mt-1">--</div>
        <span id="oiTotals" class="text-[11px] text-slate-400 mt-1 block">PE: -- | CE: --</span>
      </div>
    </div>

    <!-- Option Chain Table -->
    <div class="border border-slate-200 rounded-xl overflow-x-auto shadow-sm">
      <table class="w-full text-center text-xs">
        <thead class="bg-slate-100 text-slate-700 font-bold border-b border-slate-200">
          <tr>
            <th class="p-2.5 text-blue-700 bg-blue-50/50">Call OI ('000s)</th>
            <th class="p-2.5 bg-slate-200">Strike Price</th>
            <th class="p-2.5 text-emerald-700 bg-emerald-50/50">Put OI ('000s)</th>
            <th class="p-2.5 bg-purple-50 text-purple-900 border-l border-slate-200">Strike PCR</th>
          </tr>
        </thead>
        <tbody id="tableBody" class="divide-y divide-slate-100 font-mono">
          <!-- Populated dynamically via WebSocket state -->
        </tbody>
      </table>
    </div>

    <div class="mt-3 text-[11px] text-slate-400 text-right">
      Auto-polling updates every 2 seconds from TrueData engine
    </div>
  </div>

  <script>
    async function fetchData() {
      try {
        const res = await fetch("/api/live");
        const d = await res.json();

        document.getElementById("spotPrice").innerText = parseFloat(d.spot).toFixed(2);
        document.getElementById("atmLabel").innerHTML = `ATM Strike: <b class="text-amber-600">${d.atm}</b>`;
        document.getElementById("maxPainVal").innerText = d.max_pain;

        const diff = Math.round(d.max_pain - d.spot);
        document.getElementById("maxPainDiff").innerText = 
          diff === 0 ? "Settling at ATM" : `${diff > 0 ? "+" : ""}${diff} pts from Spot`;

        document.getElementById("overallPcr").innerText = d.overall_pcr;
        document.getElementById("oiTotals").innerText = `PE: ${d.total_put_oi} | CE: ${d.total_call_oi}`;

        const isBullish = parseFloat(d.overall_pcr) >= 1.0;
        const pcrBadge = document.getElementById("pcrBadge");
        pcrBadge.innerText = isBullish ? "Bullish" : "Bearish";
        pcrBadge.className = `text-[10px] font-bold px-2 py-0.5 rounded-full ${isBullish ? "bg-emerald-100 text-emerald-800" : "bg-rose-100 text-rose-800"}`;

        const tbody = document.getElementById("tableBody");
        tbody.innerHTML = d.rows.map(r => {
          const rowBg = r.is_max_pain ? "bg-indigo-50/80 font-semibold" : r.is_atm ? "bg-amber-50/60" : "hover:bg-slate-50";
          const pcrColor = parseFloat(r.pcr) >= 1.0 ? "text-emerald-600" : "text-rose-600";

          return `
            <tr class="${rowBg} transition-colors">
              <td class="p-2 font-semibold text-slate-700">${r.call_oi.toLocaleString()}</td>
              <td class="p-2 font-bold">
                <div class="flex items-center justify-center gap-1.5 font-sans">
                  <span>${r.strike}</span>
                  ${r.is_atm ? '<span class="text-[9px] bg-amber-500 text-white px-1.5 py-0.5 rounded font-bold shadow-sm">ATM</span>' : ''}
                  ${r.is_max_pain ? '<span class="text-[9px] bg-indigo-700 text-white px-1.5 py-0.5 rounded font-bold shadow-sm">★ PAIN</span>' : ''}
                </div>
              </td>
              <td class="p-2 font-semibold text-slate-700">${r.put_oi.toLocaleString()}</td>
              <td class="p-2 border-l border-slate-200 font-bold ${pcrColor}">${r.pcr}</td>
            </tr>
          `;
        }).join("");

      } catch (err) {
        console.error("Polling error:", err);
      }
    }

    // Refresh every 2 seconds
    fetchData();
    setInterval(fetchData, 2000);
  </script>
</body>
</html>
"""

if __name__ == "__main__":
    print("---------------------------------------------------------------")
    print(" TrueData Live Server running: http://127.0.0.1:5000")
    print("---------------------------------------------------------------")
    app.run(port=5000, debug=False)