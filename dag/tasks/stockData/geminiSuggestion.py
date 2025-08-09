import google.generativeai as genai
from google.api_core.exceptions import ResourceExhausted
from airflow.models import Variable
import json
import re
import time

MAX_REQUESTS_PER_MINUTE = 5
REQUEST_WINDOW_SECONDS = 60

def get_stock_suggestion_gemini(**kwargs):
    ti = kwargs["ti"]
    enriched_data = ti.xcom_pull(task_ids="enrich_data", key="enriched_price_data")

    if not enriched_data:
        print("[ERROR] No enriched data found for Gemini suggestions.")
        return

    genai.configure(api_key=Variable.get("GEMINI_API_KEY"))
    model = genai.GenerativeModel(model_name="gemini-2.0-flash-lite")

    symbol_to_latest_row = {row['symbol']: row for row in enriched_data}
    suggestions = []

    request_counter = 0
    window_start_time = time.time()

    def rate_limit():
        """Ensure we do not exceed 5 requests per minute."""
        nonlocal request_counter, window_start_time
        if request_counter >= MAX_REQUESTS_PER_MINUTE:
            elapsed = time.time() - window_start_time
            if elapsed < REQUEST_WINDOW_SECONDS:
                wait_time = REQUEST_WINDOW_SECONDS - elapsed
                print(f"[RATE LIMIT] Hit {MAX_REQUESTS_PER_MINUTE} requests. Sleeping for {wait_time:.2f} seconds...")
                time.sleep(wait_time)
            window_start_time = time.time()
            request_counter = 0
        request_counter += 1

    def clean_and_parse_json(raw_text):
        """Strip markdown/code fences and parse JSON."""
        raw_text = raw_text.strip()
        raw_text = re.sub(r"^```(json)?|```$", "", raw_text, flags=re.MULTILINE).strip()
        json_match = re.search(r"\{.*\}", raw_text, re.DOTALL)
        if not json_match:
            raise ValueError(f"No JSON found in response: {raw_text}")
        return json.loads(json_match.group(0))

    def call_gemini_with_retries(prompt, max_retries=3):
        """Call Gemini with retry, rate limiting, and safety checks."""
        for attempt in range(max_retries):
            try:
                rate_limit()
                response = model.generate_content(
                    prompt,
                    generation_config=genai.types.GenerationConfig(
                        temperature=0.3,
                        max_output_tokens=400,
                    )
                )
                # Safety: ensure there's a candidate with text
                if not response.candidates or not response.candidates[0].content.parts:
                    raise ValueError(f"No valid content returned. finish_reason={response.candidates[0].finish_reason if response.candidates else 'None'}")

                # Safely join only non-empty parts
                parts_text = []
                for part in response.candidates[0].content.parts:
                    if hasattr(part, "text") and part.text:
                        parts_text.append(part.text)
                raw_output = "".join(parts_text).strip()

                print("===== GEMINI RAW OUTPUT START =====")
                print(raw_output)
                print("===== GEMINI RAW OUTPUT END =====")

                return raw_output

            except (ResourceExhausted, Exception) as e:
                if '429' in str(e) or isinstance(e, ResourceExhausted):
                    wait_time = 2 ** attempt
                    print(f"[RETRY] Rate limited. Waiting {wait_time} seconds before retrying...")
                    time.sleep(wait_time)
                else:
                    raise
        return None

    for symbol, row in symbol_to_latest_row.items():
        sector = row.get('sector', 'Unknown')

        prompt = f"""
You are an expert stock analyst with access to both technical indicators and fundamental data.  
You will receive stock data for {symbol} including SMA, EMA, RSI, MACD, Bollinger Bands, ATR, OBV, MFI, P/E ratio, ROE, market cap, and sector, you must determine the sector to which the {symbol} is registerd in India's NSE exchnage. 

Follow this reasoning process before answering:
1. **Indicator Analysis**: Evaluate short-term and long-term signals from the technical indicators.  
2. **Fundamental Health**: Evaluate financial health from the fundamental metrics.  
3. **Sector Comparison**: Compare this stock’s metrics with others in the same sector.  
4. **Risk Assessment**: Consider volatility and downside risk.  

Constraints:
- You must output in JSON format with keys: "stock_symbol", "recommendation", "reason", "similar_sector_stocks".
- "recommendation" can be only: "BUY", "SELL", or "HOLD".
- "reason" must be a concise, data-driven explanation (2-4 sentences).
- "similar_sector_stocks" must be a list of 3 other stocks from the same sector that are worth considering for BUY.

Example Output:
{{
  "stock_symbol": "ABC",
  "recommendation": "BUY",
  "reason": "Stock is in an uptrend with RSI at 55, MACD bullish crossover, strong earnings growth, and P/E lower than sector average.",
  "similar_sector_stocks": ["XYZ", "LMN", "PQR"]
}}

Now, here is the stock data for {symbol}:

Stock Symbol: {row['symbol']}
Date: {row['date']}
Close: {row['close']}
SMA_20: {row.get('SMA_20')}
EMA_12: {row.get('EMA_12')}
EMA_26: {row.get('EMA_26')}
MACD: {row.get('MACD_12_26_9')}
MACD Signal: {row.get('MACDs_12_26_9')}
RSI (14): {row.get('RSI_14')}
MFI (14): {row.get('MFI_14')}
OBV: {row.get('OBV')}
ATR (14): {row.get('ATRr_14')}

Return ONLY valid JSON in this format, no extra text:
{{
  "symbol": "{row['symbol']}",
  "date": "{row['date']}",
  "suggestion": "Buy/Sell/Hold",
  "reason": "...",
  "sector": "...",
  "related_stocks": ["...", "...", "..."]
}}

"""
        try:
            text = call_gemini_with_retries(prompt)
            if not text:
                raise Exception("No response text returned.")

            try:
                suggestion_dict = clean_and_parse_json(text)
                suggestions.append(suggestion_dict)
            except Exception as parse_err:
                print(f"[WARNING] Invalid JSON for {symbol} on {row['date']}: {parse_err}")

        except Exception as e:
            print(f"[ERROR] Gemini failed for {symbol} on {row['date']}: {e}")

    if suggestions:
        ti.xcom_push(key="stock_suggestions", value=suggestions)
        print(f"[SUCCESS] Gemini generated {len(suggestions)} suggestions.")
    else:
        print("[WARNING] No valid Gemini suggestions generated.")
