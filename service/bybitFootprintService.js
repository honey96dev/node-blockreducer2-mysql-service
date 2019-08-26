import {dbTblName, bybit} from '../core/config';
import dbConn from '../core/dbConn';
import {sprintf} from 'sprintf-js';
import WebSocket from 'ws-reconnect';
import request from 'request';

let service = {
  timeoutDelay: 30000,
  socketTimeoutId: undefined,
  saveTimeoutId: undefined,
  calcTimeoutId: undefined,
  footprintTimeoutId: undefined,

  socket: undefined,
  socketLastTimestamp: undefined,
  subscribes: [],

  tradeBuffer: new Map(),
  tradeLastTimestamp: new Date(1970, 1, 1).toISOString(),
};

service.renewSocket = () => {
  const timestamp = new Date().getTime();
  if (service.socketTimeoutId) {
    clearTimeout(service.socketTimeoutId);
  }
  service.socketTimeoutId = setTimeout(service.renewSocket, service.timeoutDelay);
  if (service.socketLastTimestamp > timestamp - service.timeoutDelay) {
    console.log('bybit-downloadTrades', 'renewSocket-still alive', service.socketLastTimestamp);
    return;
  }

  const wsUrl = Boolean(bybit.testnet) ? bybit.wsUrlTestnet : bybit.wsUrlRealnet;
  service.socket = new WebSocket(wsUrl, {
    retryCount: 2, // default is 2
    reconnectInterval: 1 // default is 5
  });
  console.error('bybit-downloadTrades', 'renewSocket', service.socketLastTimestamp);

  service.socket.on('connect', () => {
    for (let subscribe of service.subscribes) {
      service.socket.send(subscribe);
    }
  });
  service.socket.on('message', (data) => {
    service.socketLastTimestamp = new Date().getTime();
    data = JSON.parse(data);

    service.onWsMessage(data);

    if (!!data.request) {
      console.log('bybit-downloadTrades', 'message', JSON.stringify(data));
      // if (!!data.request.op) {
      // }
    }
    if (!!data.topic) {
      const table = data.topic.split('.');
      if (table[0] === 'trade') {
        service.onWsTrade(data.action, data.data);
      }
    }
  });

  service.socket.on('reconnect', (data) => {
    const timestamp = new Date().toISOString();
    console.error('bybit-downloadTrades', 'reconnect', timestamp);
  });

  service.socket.on('destroyed', (data) => {
    console.error('bybit-downloadTrades', 'destroyed', timestamp);
  });

  service.socket.start();
};

service.onWsMessage = (data) => {
  // console.error('onWsMessage', JSON.stringify(data));
};

service.onWsTrade = (action, data) => {
  // console.error('onWsTrade', JSON.stringify(data));
  for (let trade of data) {
    service.tradeBuffer.set(trade['trade_id'], trade);
  }
};

service.startRead = (subscribes) => {
  service.subscribes = [];
  if (subscribes instanceof Array) {
    let query;
    query = JSON.stringify({
      op: 'subscribe',
      args: subscribes,
    });
    service.subscribes.push(query);
  }
  service.renewSocket();
};

service.saveTradesBuffer = () => {
  if (service.saveTimeoutId) {
    clearTimeout(service.saveTimeoutId);
  }

  let rows = [];
  let sql;
  service.tradeBuffer.forEach((value, key, map) => {
    // console.log(key, value)
    rows.push([
      value['trade_id'],
      value['timestamp'],
      value['side'],
      // value['size'],
      value['side'] === 'Buy' ? value['size'] : -value['size'],
      // value['price'],
      value['side'] === 'Buy' ? value['price'] : -value['price'],
      value['tick_direction'],
      value['cross_seq'],
    ]);
    if (service.tradeLastTimestamp < value['timestamp']) {
      service.tradeLastTimestamp = value['timestamp'];
    }

    if (rows.length > 512) {
      sql = sprintf("INSERT INTO `%s_%s` VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `side` = VALUES(`side`), `price` = VALUES(`price`), `tick_direction` = VALUES(`tick_direction`), `trade_id` = VALUES(`trade_id`), `cross_seq` = VALUES(`cross_seq`);", dbTblName.tradesBuffer, 'BTCUSD');
      dbConn.query(sql, [rows], (error, result, fields) => {
        if (error) {
          console.error('bybit-downloadTrades', 'tradesBuffer-save', JSON.stringify(error));
          return;
        }
        // console.error('bybit-downloadTrades', 'tradesBuffer-save', 'ok');
      });
      rows = [];
    }
  });
  service.tradeBuffer.clear();
  if (rows.length > 0) {
    sql = sprintf("INSERT INTO `%s_%s` VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `side` = VALUES(`side`), `price` = VALUES(`price`), `tick_direction` = VALUES(`tick_direction`), `trade_id` = VALUES(`trade_id`), `cross_seq` = VALUES(`cross_seq`);", dbTblName.tradesBuffer, 'BTCUSD');
    dbConn.query(sql, [rows], (error, result, fields) => {
      if (error) {
        console.error('bybit-downloadTrades', 'tradesBuffer-save', JSON.stringify(error));
        return;
      }
      // console.error('bybit-downloadTrades', 'tradesBuffer-save', 'ok');
    });
    rows = [];
  }

  service.saveTimeoutId = setTimeout(service.saveTradesBuffer, service.timeoutDelay);
  console.log('bitmexSaveTradesBuffer', service.tradeLastTimestamp);
};

service.calculateFootprint = () => {
  if (service.footprintTimeoutId) {
    clearTimeout(service.footprintTimeoutId);
  }

  let sql;
  let timestamp1;
  let timestamp2;
  timestamp1 = new Date(service.tradeLastTimestamp);
  timestamp1.setSeconds(0, 0);
  timestamp2 = new Date(timestamp1.getTime() - 2 * 60 * 60 * 1000);
  timestamp1 = timestamp1.toISOString();
  timestamp2 = timestamp2.toISOString();

  let symbol = 'BTCUSD';
  sql = sprintf("DELETE FROM `%s_%s` WHERE `timestamp` < '%s';", dbTblName.tradesBuffer, symbol, timestamp2);
  dbConn.query(sql, null, (error, result, fields) => {
  });

  timestamp1 = new Date(service.tradeLastTimestamp);
  timestamp1.setMinutes(Math.floor(timestamp1.getMinutes() / 5) * 5, 0, 0);
  timestamp2 = new Date(timestamp1.getTime() - 5 * 60 * 1000);
  timestamp1 = timestamp1.toISOString();
  timestamp2 = timestamp2.toISOString();
  sql = sprintf("SELECT '%s' `timestamp`, SIGN(`price`) * FLOOR(`price`) `price`, SIGN(`price`) `side`, COUNT(`timestamp`) `count` FROM `%s_%s` WHERE `timestamp` > '%s' AND `timestamp` <= '%s' GROUP BY FLOOR(`price`), `side` ORDER BY `price`;", timestamp1, dbTblName.tradesBuffer, symbol, timestamp2, timestamp1);
  dbConn.query(sql, null, (error, rows, fields) => {
    if (error) {
      console.error(JSON.stringify(error));
      return;
    }
    if (rows.length === 0) return;
    let data = [];
    for (let row of rows) {
      data.push([row['timestamp'], row['price'], row['side'], row['count']]);
    }
    symbol = 'XBTUSD';
    sql = sprintf("INSERT INTO `%s_%s` VALUES ? ON DUPLICATE KEY UPDATE `count` = VALUES(`count`);", dbTblName.footprint5m, symbol);
    dbConn.query(sql, [data], (error, rows, fields) => {
      if (error) {
        console.error(error);
      }
    });
  });
  service.footprintTimeoutId = setTimeout(service.calculateFootprint, service.timeoutDelay);
  console.log('calculateFootprint', service.tradeLastTimestamp);
};

module.exports = service;
