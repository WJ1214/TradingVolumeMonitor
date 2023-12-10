import json
import logging
import os
from collections import defaultdict
from typing import List
from enum import Enum
from datetime import datetime, timedelta

from binance.spot import Spot
from binance.um_futures import UMFutures

logger = logging.getLogger(__name__)

class KLineInterval(Enum):
  MIN1 = '1m'
  MIN3 = '3m'
  MIN15 = '15m'
  HOUR1 = '1h'
  HOUR4 = '4h'
  DAY1 = '1d'


class KLine():
  def __init__(self, **params):
    self.start_time = None
    self.open_price = None
    self.max_price = None
    self.min_price = None
    self.close_price = None
    self.turn_over = None
    self.end_time = None
    self.volume = None
    self.transaction_num = None
    self.buying_turn_over = None
    self.buying_volume = None
    self.ignore_param = None
    for key, value in params.items():
      setattr(self, key, value)
    self.start_time = int(self.start_time)
    self.open_price = float(self.open_price)
    self.max_price = float(self.max_price)
    self.close_price = float(self.close_price)
    self.turn_over = float(self.turn_over)
    self.end_time = int(self.end_time)
    self.volume = float(self.volume)
    self.transaction_num = int(self.transaction_num)
    self.buying_turn_over = float(self.buying_turn_over)
    self.buying_volume = float(self.buying_volume)
    self.ignore_param = str(self.ignore_param)

  def get_utc_format_start_time_str(self, delta_hour: int) -> str:
    utc_time = datetime.fromtimestamp(self.start_time / 1000)
    local_time = utc_time + timedelta(hours=delta_hour)
    return local_time
  
  def get_utc_format_end_time_str(self, delta_hour: int) -> str:
    utc_time = datetime.fromtimestamp(self.end_time / 1000)
    local_time = utc_time + timedelta(hours=delta_hour)
    return local_time
  
  def get_open_price(self):
    return self.open_price
  
  def get_max_price(self):
    return self.max_price
  
  def get_min_price(self):
    return self.min_price
  
  def get_close_price(self):
    return self.close_price

  def get_turn_over(self):
    return self.turn_over

  def get_volume(self):
    return self.volume

  def get_transaction_num(self):
    return self.transaction_num

  def get_buying_turn_over(self):
    return self.buying_turn_over

  def get_buying_volume(self):
    return self.buying_volume

  def get_ignore_param(self):
    return self.ignore_param

  def get_start_time(self):
    return self.start_time

  def get_end_time(self):
    return self.end_time


class KLines():
  def __init__(self, klines_list: List[KLine]):
    self.klines_list = klines_list
    self.klines_num = len(klines_list)
  
  def get_klines_list(self):
    return self.klines_list

  def get_klines_num(self):
    return self.klines_num
  
  def get_latest_kline(self) -> KLine:
    return self.klines_list[-1]
  
  def update_latest_klines(self, kline: KLine) -> bool:
    if self.klines_list[-1].get_end_time() <= kline.get_end_time() and self.klines_list[-1].get_start_time() == kline.get_start_time():
      self.klines_list[-1] = kline
      return True
    elif self.klines_list[-1].get_end_time() <= kline.get_end_time() and self.klines_list[-1].get_start_time() < kline.get_start_time():
      self.klines_list.pop(0)
      self.klines_list.append(kline)
      return True
    else:
      logger.error(
          f'''Update kline's start time and end time should be less than last kline.
            Last kline's start time <= given kline start time: {self.klines_list[-1].get_start_time() <= kline.get_start_time()},
            Last kline's end time <= given kline end time: {self.klines_list[-1].get_end_time() <= kline.get_end_time()}''')
      return False

  def calculate_average_buying_volume_by_given_window(self, start_index: int, window_size: int):
    assert start_index + window_size <= self.klines_num, "start_index + window_size > klines_num"
    assert window_size >= 1, "window_size < 1"
    all_buying_volume = 0
    for i in range(start_index, start_index + window_size):
      all_buying_volume += self.klines_list[i].get_buying_volume()
    return all_buying_volume / window_size


class RankMaintainer():
  def __init__(self, past_klines_window_size: int, recent_klines_window_size: int,
               max_klines_num: int,
               time_zone: int,
               client: Spot,
               um_client: UMFutures,
               use_volume_diff_ratio: bool = True,
               cache_config_path: str = './cache_config.json'):
    assert past_klines_window_size <= max_klines_num
    assert recent_klines_window_size <= max_klines_num
    self.past_klines_window_size = past_klines_window_size
    self.recent_klines_window_size = recent_klines_window_size
    self.max_klines_num = max_klines_num
    self.time_zone = time_zone
    self.use_volume_diff_ratio = use_volume_diff_ratio
    self.client = client
    self.um_client = um_client
    self.cache_content = defaultdict(list)
    self.cache_config_path = cache_config_path
    if cache_config_path:
      if os.path.exists(cache_config_path):
        with open(cache_config_path, 'r', encoding='utf-8') as f:
          self.cache_content = json.load(f)
      else:
        with open(cache_config_path, 'w', encoding='utf-8') as f:
          json.dump(self.cache_content, f)
    self.pair_name_to_klines_dict = defaultdict()
    self.get_need_trading_pair_set()
  
  def get_need_trading_pair_set(self):
    if self.cache_content and len(self.cache_content["trading_pair_names"]) != 0:
      logger.info(f"Loading cache config trading pair names.")
    pair_infos = self.client.exchange_info()
    um_pair_infos = self.um_client.exchange_info()
    pair_names = set([p["symbol"] for p in pair_infos["symbols"] if p["symbol"] and
                      p["symbol"][-4:] == 'USDT' and
                      p["status"] == 'TRADING' and
                      (p["symbol"] not in self.cache_content["trading_pair_names"])])
    um_pair_names = set([p["pair"] for p in um_pair_infos["symbols"] if p["pair"] and
                         p["pair"][-4:] == 'USDT' and
                         (p["pair"] not in self.cache_content["trading_pair_names"])])
    self.cache_content["trading_pair_names"] += list(um_pair_names & pair_names)
    with open(self.cache_config_path, 'w', encoding='utf-8') as f:
      json.dump(self.cache_content, f)
  
  def convert_klines_list_to_klines(self, klines_list: list) -> KLines:
    res = []
    for kline in klines_list:
      res.append(KLine(start_time=kline[0],
                       open_price=kline[1],
                       max_price=kline[2],
                       min_price=kline[3],
                       close_price=kline[4],
                       turn_over=kline[5],
                       end_time=kline[6],
                       volume=kline[7],
                       transaction_num=kline[8],
                       buying_turn_over=kline[9],
                       buying_volume=kline[10],
                       ignore_param=kline[11]))
    return KLines(klines_list=res)
  
  def get_current_trading_klines(self, symbol: str, interval: KLineInterval) -> KLines:
    if symbol in self.pair_name_to_klines_dict.keys() and self.pair_name_to_klines_dict[symbol]:
      return self.pair_name_to_klines_dict[symbol]
    current_trading_klines_list = self.client.klines(symbol, interval.value, timeZone=str(self.time_zone), limit=self.max_klines_num)
    current_trading_klines = self.convert_klines_list_to_klines(current_trading_klines_list)
    self.pair_name_to_klines_dict[symbol] = current_trading_klines
    return self.pair_name_to_klines_dict[symbol]
  
  def get_buying_diff_ration_volume_rank(self, interval: KLineInterval, top_k: int):
    rank_pair_names = self.cache_content["trading_pair_names"]
    volume_diff_ratio_list = []
    
    # Get init trading klines and calculate buying volume diff ratio.
    for pair_name in rank_pair_names:
      current_trading_klines = self.get_current_trading_klines(pair_name, interval)
      past_avg_buying_volume =\
          current_trading_klines.calculate_average_buying_volume_by_given_window(self.max_klines_num - self.past_klines_window_size, self.past_klines_window_size)
      recent_avg_buying_volume =\
          current_trading_klines.calculate_average_buying_volume_by_given_window(self.max_klines_num - self.recent_klines_window_size, self.recent_klines_window_size)
      buying_volume_diff = recent_avg_buying_volume - past_avg_buying_volume
      if past_avg_buying_volume == 0:
        buying_volume_diff_ratio = 0
      else:
        buying_volume_diff_ratio = buying_volume_diff / past_avg_buying_volume
      volume_diff_ratio_list.append((pair_name, buying_volume_diff_ratio))

    # Sort volume diff ratio list and return top k.
    sorted_volume_diff_ratio_list = sorted(volume_diff_ratio_list, key=lambda x: x[1], reverse=True)
    return sorted_volume_diff_ratio_list[:top_k]

if __name__ == '__main__':
  client = Spot()
  um_client = UMFutures()
  rm = RankMaintainer(past_klines_window_size=20, recent_klines_window_size=3,
                      max_klines_num=50, time_zone=8, client=client, um_client=um_client)
  print(rm.get_buying_diff_ration_volume_rank(KLineInterval.HOUR1, top_k=10))