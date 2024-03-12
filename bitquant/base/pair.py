from dataclasses import dataclass, asdict
from functools import reduce
from typing import Union, Dict, List, Tuple, Any
from bitquant.data.data_client import DataClient
from bitquant.data.exchange import BinanceExchange
from bitquant.data.utils import TimeUtils

Values = Union[int, float]

def get_available_pairs(exchange=BinanceExchange, base="USDT"):
    client = DataClient(exchange)
    symbol_info = client.get_symbol_info()
    usdt_pairs = symbol_info.loc[symbol_info["symbol"].apply(lambda x: x.endswith(base)), "symbol"].tolist()
    return usdt_pairs

TRADABLE_PAIRS = ('BTCUSDT', 'ETHUSDT', 'BCHUSDT', 'XRPUSDT', 'EOSUSDT', 'LTCUSDT', 'TRXUSDT',
                'ETCUSDT', 'LINKUSDT', 'XLMUSDT', 'ADAUSDT', 'XMRUSDT', 'DASHUSDT', 'ZECUSDT',
                'XTZUSDT', 'BNBUSDT', 'ATOMUSDT', 'ONTUSDT', 'IOTAUSDT', 'BATUSDT', 'VETUSDT',
                'NEOUSDT', 'QTUMUSDT', 'IOSTUSDT', 'THETAUSDT', 'ALGOUSDT', 'ZILUSDT', 'KNCUSDT',
                'ZRXUSDT', 'COMPUSDT', 'OMGUSDT', 'DOGEUSDT', 'SXPUSDT', 'KAVAUSDT', 'BANDUSDT',
                'RLCUSDT', 'WAVESUSDT', 'MKRUSDT', 'SNXUSDT', 'DOTUSDT', 'DEFIUSDT', 'YFIUSDT',
                'BALUSDT', 'CRVUSDT', 'TRBUSDT', 'RUNEUSDT', 'SUSHIUSDT', 'SRMUSDT', 'EGLDUSDT',
                'SOLUSDT', 'ICXUSDT', 'STORJUSDT', 'BLZUSDT', 'UNIUSDT', 'AVAXUSDT', 'FTMUSDT',
                'HNTUSDT', 'ENJUSDT', 'FLMUSDT', 'TOMOUSDT', 'RENUSDT', 'KSMUSDT', 'NEARUSDT',
                'AAVEUSDT', 'FILUSDT', 'RSRUSDT', 'LRCUSDT', 'MATICUSDT', 'OCEANUSDT', 'CVCUSDT',
                'BELUSDT', 'CTKUSDT', 'AXSUSDT', 'ALPHAUSDT', 'ZENUSDT', 'SKLUSDT', 'GRTUSDT',
                '1INCHUSDT', 'CHZUSDT', 'SANDUSDT', 'ANKRUSDT', 'BTSUSDT', 'LITUSDT', 'UNFIUSDT',
                'REEFUSDT', 'RVNUSDT', 'SFPUSDT', 'XEMUSDT', 'BTCSTUSDT', 'COTIUSDT', 'CHRUSDT',
                'MANAUSDT', 'ALICEUSDT', 'HBARUSDT', 'ONEUSDT', 'LINAUSDT', 'STMXUSDT', 'DENTUSDT',
                'CELRUSDT', 'HOTUSDT', 'MTLUSDT', 'OGNUSDT', 'NKNUSDT', 'SCUSDT', 'DGBUSDT',
                '1000SHIBUSDT', 'BAKEUSDT', 'GTCUSDT', 'BTCDOMUSDT', 'IOTXUSDT', 'AUDIOUSDT',
                'RAYUSDT', 'C98USDT', 'MASKUSDT', 'ATAUSDT', 'DYDXUSDT', '1000XECUSDT', 'GALAUSDT',
                'CELOUSDT', 'ARUSDT', 'KLAYUSDT', 'ARPAUSDT', 'CTSIUSDT', 'LPTUSDT', 'ENSUSDT',
                'PEOPLEUSDT', 'ANTUSDT', 'ROSEUSDT', 'DUSKUSDT', 'FLOWUSDT', 'IMXUSDT', 'API3USDT',
                'GMTUSDT', 'APEUSDT', 'WOOUSDT', 'FTTUSDT', 'JASMYUSDT', 'DARUSDT', 'GALUSDT',
                'OPUSDT', 'INJUSDT', 'STGUSDT', 'FOOTBALLUSDT', 'SPELLUSDT', '1000LUNCUSDT',
                'LUNA2USDT', 'LDOUSDT', 'CVXUSDT', 'ICPUSDT', 'APTUSDT', 'QNTUSDT', 'BLUEBIRDUSDT',
                'FETUSDT', 'FXSUSDT', 'HOOKUSDT', 'MAGICUSDT', 'TUSDT', 'RNDRUSDT', 'HIGHUSDT',
                'MINAUSDT', 'ASTRUSDT', 'AGIXUSDT', 'PHBUSDT', 'GMXUSDT', 'CFXUSDT', 'STXUSDT',
                'COCOSUSDT', 'BNXUSDT', 'ACHUSDT', 'SSVUSDT', 'CKBUSDT', 'PERPUSDT', 'TRUUSDT',
                'LQTYUSDT', 'USDCUSDT', 'IDUSDT', 'ARBUSDT', 'JOEUSDT', 'TLMUSDT', 'AMBUSDT',
                'LEVERUSDT', 'RDNTUSDT', 'HFTUSDT', 'XVSUSDT', 'BLURUSDT', 'EDUUSDT', 'IDEXUSDT',
                'SUIUSDT', '1000PEPEUSDT', '1000FLOKIUSDT', 'UMAUSDT', 'RADUSDT', 'KEYUSDT',
                'COMBOUSDT', 'NMRUSDT', 'MAVUSDT', 'MDTUSDT', 'XVGUSDT', 'WLDUSDT', 'PENDLEUSDT',
                'ARKMUSDT', 'AGLDUSDT', 'YGGUSDT', 'DODOXUSDT', 'BNTUSDT', 'OXTUSDT', 'SEIUSDT',
                'CYBERUSDT', 'HIFIUSDT', 'ARKUSDT', 'FRONTUSDT', 'GLMRUSDT', 'BICOUSDT', 'STRAXUSDT',
                'LOOMUSDT', 'BIGTIMEUSDT', 'BONDUSDT', 'ORBSUSDT', 'STPTUSDT', 'WAXPUSDT', 'BSVUSDT',
                'RIFUSDT', 'POLYXUSDT', 'GASUSDT', 'POWRUSDT', 'SLPUSDT', 'TIAUSDT', 'SNTUSDT',
                'CAKEUSDT', 'MEMEUSDT', 'TWTUSDT', 'TOKENUSDT', 'ORDIUSDT', 'STEEMUSDT', 'BADGERUSDT',
                'ILVUSDT', 'NTRNUSDT', 'MBLUSDT', 'KASUSDT', 'BEAMXUSDT', '1000BONKUSDT', 'PYTHUSDT',
                'SUPERUSDT', 'USTCUSDT', 'ONGUSDT', 'ETHWUSDT', 'JTOUSDT', '1000SATSUSDT', 'AUCTIONUSDT',
                '1000RATSUSDT', 'ACEUSDT', 'MOVRUSDT', 'NFPUSDT', 'AIUSDT', 'XAIUSDT', 'WIFUSDT',
                'MANTAUSDT', 'ONDOUSDT', 'LSKUSDT', 'ALTUSDT', 'JUPUSDT', 'ZETAUSDT', 'RONINUSDT',
                'DYMUSDT', 'OMUSDT', 'PIXELUSDT', 'STRKUSDT', 'MAVIAUSDT', 'GLMUSDT', 'PORTALUSDT',
                'TONUSDT', 'AXLUSDT', 'MYROUSDT')

class SymbolValueDict(dict):
    """
    Stores the portfolio of current pair holdings
    SymbolValueDict is meant to be immutable
    The only method available is update_portfolio() which creates a new SymbolValueDict instance

    Initialization:
        - SymbolValueDict(values=[-1, 0.5, 0.1, ...])
            -- values here has to be in the same order as TRADABLE_PAIRS
        - SymbolValueDict(values=tuple([-1, 0.5, 0.1, ...]))
            -- values here has to be in the same order as TRADABLE_PAIRS
        - SymbolValueDict(values={"BTCUSDT": -1, "ETHUSDT": 0.5, ...})
            -- keys here doesn't have to be in order
        - SymbolValueDict({})  # creates a SymbolValueDict with
    """
    pairs = TRADABLE_PAIRS

    def __init__(self, values: Union[Dict[str, Values], List[Values], Tuple[Values, ...]]):
        if isinstance(values, dict):
            if not all((failed_key := k) in self.pairs for k in values.keys()):
                raise KeyError(f"Key={failed_key} not in TRADABLE_PAIRS")
            super().__init__({p:values.get(p, 0) for p in self.pairs})
        elif isinstance(values, (list, tuple)):
            if len(values) != len(self.pairs):
                raise IndexError(f"values length has to be the same as length of TRADABLE_PAIRS")
            super().__init__({p:values[i] for i,p in enumerate(self.pairs)})
        else:
            raise TypeError(f"unexpected type {type(values)}")

    def update_portfolio(self, changes:Union[List[Dict], Dict[str, Union[int, float]]]) -> 'SymbolValueDict':
        if isinstance(changes, dict):
            return SymbolValueDict({**self, **changes})
        elif isinstance(changes, list) and all(isinstance(d, dict) for d in changes):
            return reduce(lambda a,b: a.update(b), [self] + changes)
        else:
            raise TypeError("cannot update portfolio with changes")

    def __setitem__(self, __key: Any, __value: Any) -> None:
        raise AttributeError("SymbolValueDict class is immutable")
    def __delitem__(self, __key: Any) -> None:
        raise AttributeError("SymbolValueDict class is immutable")


@dataclass(frozen=True)
class PortfolioRecord:
    def __init__(self, svdict: SymbolValueDict):
        self.portfolio = svdict
        self.timestamp_ms = TimeUtils.now_in_ms()

    def __dict__(self) -> Dict:
        return asdict(self)


if __name__ == "__main__":
    ...
    # print(get_available_pairs(BinanceExchange))
    a = SymbolValueDict({})
    print(a)