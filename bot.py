import pandas as pd
from binance_common.configuration import ConfigurationRestAPI
from binance_common.constants import DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL
from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
    DerivativesTradingUsdsFutures,
)


class BinanceBot:
    def __init__(self, api_key: str, api_secret: str):
        configuration = ConfigurationRestAPI(
            api_key=api_key,
            api_secret=api_secret,
            base_path=DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL,
        )
        self.client = DerivativesTradingUsdsFutures(config_rest_api=configuration)

    def get_funding_rate(self, assets=None):
        """
        Funding rate for USDT pairs
        """
        data = self.client.rest_api.mark_price().data().model_dump()["actual_instance"]
        if assets:
            filtered = [i for i in data if i["symbol"] in assets]
        else:
            filtered = [i for i in data if i["symbol"].endswith("USDT")]
        sort = sorted(filtered, key=lambda x: x["symbol"])
        dicts = {i["symbol"]: [round(float(i["last_funding_rate"]), 6), i["next_funding_time"]] for i in sort}
        df = (
            pd.DataFrame(dicts)
            .T.rename(columns={0: "funding_rate", 1: "next_funding_time"})
            .reset_index(names=["symbol"])
            .sort_values("funding_rate")
        )
        df["next_funding_time"] = (
            pd.to_datetime(df["next_funding_time"], unit="ms")
            .dt.tz_localize("UTC")
            .dt.tz_convert("America/Mexico_City")
            .dt.strftime("%Y-%m-%d %H:%M")
        )
        df["side"] = df["funding_rate"].apply(lambda x: "BUY" if x <= 0 else "SELL")
        df["ranking"] = df["funding_rate"].abs().rank(ascending=False)
        return df.sort_values("ranking")

    def get_perpetual_symbols(self):
        """
        Get all perpetual symbols
        """
        inf = self.client.rest_api.exchange_information().data().model_dump()
        symbols = [
            i["symbol"] for i in inf["symbols"] if i["contract_type"] == "PERPETUAL" and i["symbol"].endswith("USDT")
        ]
        return symbols

    def get_leverage_catalog(self):
        inf = self.client.rest_api.exchange_information().data().model_dump()
        symbols = [
            i["symbol"] for i in inf["symbols"] if i["contract_type"] == "PERPETUAL" and i["symbol"].endswith("USDT")
        ]
        lev = self.client.rest_api.notional_and_leverage_brackets().data().model_dump()["actual_instance"]
        lev = [
            {"symbol": item["symbol"], **bracket}
            for item in lev
            for bracket in item["brackets"]
            if item["symbol"] in symbols
        ]
        lev = pd.DataFrame(lev)[["symbol", "initial_leverage", "notional_cap"]]
        return lev

    def get_funding_history(self, symbol=None):
        funding_fee_hist = self.client.rest_api.get_income_history(
            income_type="FUNDING_FEE", symbol=symbol, limit=1000
        ).data()
        funding_fee_hist = [i.model_dump() for i in funding_fee_hist]
        df = pd.DataFrame(funding_fee_hist)[["symbol", "time", "income", "income_type"]].sort_values(
            ["time", "symbol"], ascending=[False, True]
        )
        df["time"] = (
            pd.to_datetime(df["time"], unit="ms")
            .dt.tz_localize("UTC")
            .dt.tz_convert("America/Mexico_City")
            .dt.strftime("%Y-%m-%d %H:%M")
        )
        df["income"] = round(df["income"].astype(float), 2)
        return df

    def _calculate_quantity(self, symbol, price, leverage, margin):
        price = float(self.client.mark_price(symbol).data().model_dump()["mark_price"])
        quantity = (margin * leverage) / price
        return quantity

    def get_past_funding_rate(self, symbol, limit=None):
        """Get past funding rate data for a specific symbol."""
        fr = pd.DataFrame(
            map(
                lambda x: x.model_dump(),
                self.client.rest_api.get_funding_rate_history(symbol=symbol, limit=limit).data(),
            )
        )
        fr["funding_time"] = pd.to_datetime(fr["funding_time"], unit="ms")
        fr["funding_time"] = (
            fr["funding_time"].dt.tz_localize("UTC").dt.tz_convert("America/Mexico_City").dt.strftime("%Y-%m-%d %H:00")
        )
        fr["funding_rate"] = fr["funding_rate"].astype(float)
        fr = fr.sort_values("funding_time", ascending=False)
        return fr

    def get_funding_arbitrage(self, leverage, entry_market=False, exit_market=True):
        lev = self.get_leverage_catalog()
        lev = lev[lev.initial_leverage == leverage].rename(
            columns={"initial_leverage": "leverage", "notional_cap": "position"}
        )
        fr = self.get_funding_rate()
        base = fr.merge(lev, on="symbol", how="left")
        base["percent_profit"] = abs(base["funding_rate"]) * base["leverage"]
        base["margin"] = base["position"] / base["leverage"]
        fee_entry = 0.0005 if entry_market else 0.0002
        fee_exit = 0.0005 if exit_market else 0.0002
        base["fees"] = base["position"] * (fee_entry + fee_exit)
        base["gross_profit"] = round(base["position"] * abs(base["funding_rate"]), 2)
        base["net_profit"] = base["gross_profit"] - base["fees"]
        return base

    def get_positions(self, reduced_cols=False):
        positions = pd.DataFrame(map(lambda x: x.model_dump(), self.client.rest_api.position_information_v3().data()))
        positions["side"] = positions["position_amt"].apply(lambda x: "LONG" if float(x) > 0 else "SHORT")
        positions["un_realized_profit"] = positions["un_realized_profit"].astype(float).round(2)
        positions["isolated_wallet"] = positions["isolated_wallet"].astype(float).round(2)
        cols = ["symbol", "side", "entry_price", "un_realized_profit", "isolated_wallet", "mark_price"]
        return positions[cols] if reduced_cols else positions

    def create_scaled_trailing_stop_order(
        self,
        symbol: str,
        side: str,
        size_usd: float,
        activation_price: float,
        callback_rate: float,
        reduce_only: bool = False,
    ):
        """
        Create a scaled trailing stop order.
        """
        quantity = round(size_usd / activation_price, 2)
        order = self.client.rest_api.new_order(
            symbol=symbol,
            side=side,
            type="TRAILING_STOP_MARKET",
            quantity=quantity,
            activation_price=activation_price,
            callback_rate=callback_rate,
            reduce_only=reduce_only,
        ).data()
        return order.model_dump()

    def cancel_all_trailing_stop_orders(self, symbol: str):
        """
        Cancel all trailing stop orders for a given symbol.
        """

        # Get first only trailing stop orders
        curr_trl_ords = self.client.rest_api.current_all_open_orders(symbol=symbol).data()
        curr_trl_ords = [i.model_dump() for i in curr_trl_ords]
        curr_trl_ords = [i["order_id"] for i in curr_trl_ords if i["type"] == "TRAILING_STOP_MARKET"]
        print(f"Found {len(curr_trl_ords)} trailing stop orders to cancel.")
        for ord_i in curr_trl_ords:
            print(f"Cancelling order ID: {ord_i}")
            self.client.rest_api.cancel_order(symbol=symbol, order_id=ord_i)
