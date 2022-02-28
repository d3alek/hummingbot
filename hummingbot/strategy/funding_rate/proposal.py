from decimal import Decimal
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.core.event.events import FundingInfo

s_decimal_nan = Decimal("NaN")
s_decimal_0 = Decimal("0")


class ProposalSide:
    """
    An arbitrage proposal side which contains info needed for order submission.
    """
    def __init__(self,
                 market_info: MarketTradingPairTuple,
                 is_buy: bool,
                 order_price: Decimal,
                 funding_info: FundingInfo
                 ):
        """
        :param market_info: The market where to submit the order
        :param is_buy: True if buy order
        :param order_price: The price required for order submission, this could differ from the quote price
        """
        self.market_info: MarketTradingPairTuple = market_info
        self.is_buy: bool = is_buy
        self.order_price: Decimal = order_price
        self.funding_info = funding_info

    def __repr__(self):
        side = "Buy" if self.is_buy else "Sell"
        base, quote = self.market_info.trading_pair.split("-")
        return f"{self.market_info.market.display_name.capitalize()}: {side} {base}" \
               f" at {self.order_price} {quote} Funding Rate ({self.funding_info.rate})."


class Proposal:
    """
    An arbitrage proposal which contains 2 sides of the proposal - one on spot market and one on perpetual market.
    """
    def __init__(self,
                 side1: ProposalSide,
                 side2: ProposalSide,
                 order_amount: Decimal):
        """
        Creates ArbProposal
        :param spot_side: An ArbProposalSide on spot market
        :param perp_side: An ArbProposalSide on perpetual market
        :param order_amount: An order amount for both spot and perpetual market
        """
        if side1.is_buy == side2.is_buy:
            raise Exception("Proposals cannot be on the same side.")
        self.buy_side, self.sell_side = (side1, side2) if side1.is_buy else (side2, side1)
        self.order_amount: Decimal = order_amount  # TODO auto-determine based on limit order

    def profit_pct(self) -> Decimal:
        """
        Calculates and returns profit (in percentage value).
        """
        # TODO make this accurate
        buy_price = self.buy_side.funding_info.rate
        sell_price = self.sell_side.funding_info.rate
        return (sell_price - buy_price) / buy_price

    def __repr__(self):
        return f"Buy Side: {self.buy_side}\nSell Side: {self.sell_side}\nOrder amount: {self.order_amount}\n"
