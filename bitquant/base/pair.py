from dataclasses import dataclass, asdict
from typing import Union, Dict, List, Tuple, Any
from bitquant.data.data_client import DataClient
from bitquant.data.exchange import BinanceExchange

Values = Union[int, float]

def get_available_pairs(exchange=BinanceExchange, base="USDT"):
    client = DataClient(exchange)
    symbol_info = client.get_symbol_info()
    usdt_pairs = symbol_info.loc[symbol_info["symbol"].apply(lambda x: x.endswith(base)), "symbol"].tolist()
    return usdt_pairs

TRADABLE_PAIRS = ('BTCUSDT', 'ETHUSDT', 'BCHUSDT', 'XRPUSDT', 'EOSUSDT', 'LTCUSDT', 'TRXUSDT', 'ETCUSDT', 'LINKUSDT', 'XLMUSDT',
                  'ADAUSDT', 'XMRUSDT', 'DASHUSDT', 'ZECUSDT', 'XTZUSDT', 'BNBUSDT', 'ATOMUSDT', 'ONTUSDT', 'IOTAUSDT', 'BATUSDT',
                  'VETUSDT', 'NEOUSDT', 'QTUMUSDT', 'IOSTUSDT', 'THETAUSDT', 'ALGOUSDT', 'ZILUSDT', 'KNCUSDT', 'ZRXUSDT',
                  'COMPUSDT', 'OMGUSDT', 'DOGEUSDT', 'SXPUSDT', 'KAVAUSDT', 'BANDUSDT', 'RLCUSDT', 'WAVESUSDT', 'MKRUSDT',
                  'SNXUSDT', 'DOTUSDT', 'DEFIUSDT', 'YFIUSDT', 'BALUSDT', 'CRVUSDT', 'TRBUSDT', 'RUNEUSDT', 'SUSHIUSDT', 'EGLDUSDT',
                  'SOLUSDT', 'ICXUSDT', 'STORJUSDT', 'BLZUSDT', 'UNIUSDT', 'AVAXUSDT', 'FTMUSDT', 'ENJUSDT', 'FLMUSDT', 'RENUSDT',
                  'KSMUSDT', 'NEARUSDT', 'AAVEUSDT', 'FILUSDT', 'RSRUSDT', 'LRCUSDT', 'MATICUSDT', 'OCEANUSDT', 'BELUSDT', 'CTKUSDT',
                  'AXSUSDT', 'ALPHAUSDT', 'ZENUSDT', 'SKLUSDT', 'GRTUSDT', '1INCHUSDT', 'CHZUSDT', 'SANDUSDT', 'ANKRUSDT', 'LITUSDT',
                  'UNFIUSDT', 'REEFUSDT', 'RVNUSDT', 'SFPUSDT', 'XEMUSDT', 'COTIUSDT', 'CHRUSDT', 'MANAUSDT', 'ALICEUSDT', 'HBARUSDT',
                  'ONEUSDT', 'LINAUSDT', 'STMXUSDT', 'DENTUSDT', 'CELRUSDT', 'HOTUSDT', 'MTLUSDT', 'OGNUSDT', 'NKNUSDT', 'DGBUSDT',
                  '1000SHIBUSDT', 'BAKEUSDT', 'GTCUSDT', 'BTCDOMUSDT', 'IOTXUSDT', 'AUDIOUSDT', 'C98USDT', 'MASKUSDT', 'ATAUSDT',
                  'DYDXUSDT', '1000XECUSDT', 'GALAUSDT', 'CELOUSDT', 'ARUSDT', 'KLAYUSDT', 'ARPAUSDT', 'CTSIUSDT', 'LPTUSDT', 'ENSUSDT',
                  'PEOPLEUSDT', 'ANTUSDT', 'ROSEUSDT', 'DUSKUSDT', 'FLOWUSDT', 'IMXUSDT', 'API3USDT', 'GMTUSDT', 'APEUSDT', 'WOOUSDT',
                  'JASMYUSDT', 'DARUSDT', 'GALUSDT', 'OPUSDT', 'INJUSDT', 'STGUSDT', 'FOOTBALLUSDT', 'SPELLUSDT', '1000LUNCUSDT',
                  'LUNA2USDT', 'LDOUSDT', 'CVXUSDT', 'ICPUSDT', 'APTUSDT', 'QNTUSDT', 'BLUEBIRDUSDT', 'FETUSDT', 'FXSUSDT', 'HOOKUSDT',
                  'MAGICUSDT', 'TUSDT', 'RNDRUSDT', 'HIGHUSDT', 'MINAUSDT', 'ASTRUSDT', 'AGIXUSDT', 'PHBUSDT', 'GMXUSDT', 'CFXUSDT',
                  'STXUSDT', 'BNXUSDT', 'ACHUSDT', 'SSVUSDT', 'CKBUSDT', 'PERPUSDT', 'TRUUSDT', 'LQTYUSDT', 'USDCUSDT', 'IDUSDT',
                  'ARBUSDT', 'JOEUSDT', 'TLMUSDT', 'AMBUSDT', 'LEVERUSDT', 'RDNTUSDT', 'HFTUSDT', 'XVSUSDT', 'ETHBTC', 'BLURUSDT',
                  'EDUUSDT', 'IDEXUSDT', 'SUIUSDT', '1000PEPEUSDT', '1000FLOKIUSDT', 'UMAUSDT', 'RADUSDT', 'KEYUSDT', 'COMBOUSDT',
                  'NMRUSDT', 'MAVUSDT', 'MDTUSDT', 'XVGUSDT', 'WLDUSDT', 'PENDLEUSDT', 'ARKMUSDT', 'AGLDUSDT', 'YGGUSDT', 'DODOXUSDT',
                  'BNTUSDT', 'OXTUSDT', 'SEIUSDT', 'CYBERUSDT', 'HIFIUSDT', 'ARKUSDT', 'FRONTUSDT', 'GLMRUSDT', 'BICOUSDT', 'STRAXUSDT',
                  'LOOMUSDT', 'BIGTIMEUSDT', 'BONDUSDT', 'ORBSUSDT', 'STPTUSDT', 'WAXPUSDT', 'BSVUSDT', 'RIFUSDT', 'POLYXUSDT', 'GASUSDT',
                  'POWRUSDT', 'SLPUSDT', 'TIAUSDT', 'SNTUSDT', 'CAKEUSDT', 'MEMEUSDT', 'TWTUSDT', 'TOKENUSDT', 'ORDIUSDT', 'STEEMUSDT',
                  'BADGERUSDT', 'ILVUSDT', 'NTRNUSDT', 'MBLUSDT', 'KASUSDT', 'BEAMXUSDT', '1000BONKUSDT', 'PYTHUSDT', 'SUPERUSDT',
                  'USTCUSDT', 'ONGUSDT', 'ETHWUSDT', 'JTOUSDT', '1000SATSUSDT', 'AUCTIONUSDT', '1000RATSUSDT', 'ACEUSDT', 'MOVRUSDT',
                  'NFPUSDT', 'BTCUSDC', 'ETHUSDC', 'BNBUSDC', 'SOLUSDC', 'XRPUSDC', 'AIUSDT', 'XAIUSDT', 'DOGEUSDC', 'WIFUSDT', 'MANTAUSDT',
                  'ONDOUSDT', 'LSKUSDT', 'ALTUSDT', 'JUPUSDT', 'ZETAUSDT', 'RONINUSDT', 'DYMUSDT', 'SUIUSDC', 'OMUSDT', 'LINKUSDC',
                  'PIXELUSDT', 'STRKUSDT', 'MAVIAUSDT', 'ORDIUSDC', 'GLMUSDT', 'PORTALUSDT', 'TONUSDT', 'AXLUSDT', 'MYROUSDT',
                  '1000PEPEUSDC', 'METISUSDT', 'AEVOUSDT')


if __name__ == "__main__":
    ...
    # print(get_available_pairs(BinanceExchange))
    # a = SymbolValueDict({})
    # print(a)