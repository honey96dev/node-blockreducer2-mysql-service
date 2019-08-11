import {dbTblName, bitmex} from '../core/config';
import dbConn from '../core/dbConn';
import {sprintf} from 'sprintf-js';
import WebSocket from 'ws-reconnect';
import request from 'request';

let service = {
  timeoutDelay: 30000,
  socketTimeoutId: undefined,
  saveTimeoutId: undefined,
  calcTimeoutId: undefined,

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
    console.log('bitmexCalculateVolume', 'renewSocket-still alive', service.socketLastTimestamp);
    return;
  }

  const wsUrl = Boolean(bitmex.testnet) ? bitmex.wsUrlTestnet : bitmex.wsUrlRealnet;
  service.socket = new WebSocket(wsUrl, {
    retryCount: 2, // default is 2
    reconnectInterval: 1 // default is 5
  });
  console.error('bitmexCalculateVolume', 'renewSocket', service.socketLastTimestamp);

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
      console.log('bitmexCalculateVolume', 'message', JSON.stringify(data));
      // if (!!data.request.op) {
      // }
    }
    if (!!data.table) {
      const table = data.table;
      if (table === 'trade') {
        service.onWsTrade(data.action, data.data);
      }
    }
  });

  service.socket.on('reconnect', (data) => {
    const timestamp = new Date().toISOString();
    console.error('bitmexCalculateVolume', 'reconnect', timestamp);
  });

  service.socket.on('destroyed', (data) => {
    console.error('bitmexCalculateVolume', 'destroyed', timestamp);
  });

  service.socket.start();
};

service.onWsMessage = (data) => {
  // console.error(JSON.stringify(data));
};

service.onWsTrade = (action, data) => {
  // console.error(JSON.stringify(data));
  for (let trade of data) {
    service.tradeBuffer.set(trade['trdMatchID'], trade);
  }
};

service.startRead = (subscribes) => {
  service.subscribes = [];
  if (subscribes instanceof Array) {
    let query;
    for (let subscribe of subscribes) {
      query = JSON.stringify({
        op: 'subscribe',
        args: subscribe,
      });
      service.subscribes.push(query);
    }
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
      value['trdMatchID'],
      value['timestamp'],
      value['symbol'],
      value['side'],
      // value['size'],
      value['side'] === 'Buy' ? value['size'] : -value['size'],
      // value['price'],
      value['side'] === 'Buy' ? value['price'] : -value['price'],
      value['tickDirection'],
      value['grossValue'],
      value['homeNotional'],
      value['foreignNotional']
    ]);
    if (service.tradeLastTimestamp < value['timestamp']) {
      service.tradeLastTimestamp = value['timestamp'];
    }

    if (rows.length > 512) {
      sql = sprintf("INSERT INTO `%s_%s`(`trdMatchID`, `timestamp`, `symbol`, `side`, `size`, `price`, `tickDirection`, `grossValue`, `homeNotional`, `foreignNotional`) VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `symbol` = VALUES(`symbol`), `side` = VALUES(`side`), `price` = VALUES(`price`), `tickDirection` = VALUES(`tickDirection`), `grossValue` = VALUES(`grossValue`), `homeNotional` = VALUES(`homeNotional`), `foreignNotional` = VALUES(`foreignNotional`);", dbTblName.tradesBuffer, 'XBTUSD');
      dbConn.query(sql, [rows], (error, result, fields) => {
        if (error) {
          console.error('bitmexCalculateVolume', 'tradesBuffer-save', JSON.stringify(error));
          return;
        }
      });
      rows = [];
    }
  });
  service.tradeBuffer.clear();
  if (rows.length > 0) {
    sql = sprintf("INSERT INTO `%s_%s`(`trdMatchID`, `timestamp`, `symbol`, `side`, `size`, `price`, `tickDirection`, `grossValue`, `homeNotional`, `foreignNotional`) VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `symbol` = VALUES(`symbol`), `side` = VALUES(`side`), `price` = VALUES(`price`), `tickDirection` = VALUES(`tickDirection`), `grossValue` = VALUES(`grossValue`), `homeNotional` = VALUES(`homeNotional`), `foreignNotional` = VALUES(`foreignNotional`);", dbTblName.tradesBuffer, 'XBTUSD');
    dbConn.query(sql, [rows], (error, result, fields) => {
      if (error) {
        console.error('bitmexCalculateVolume', 'tradesBuffer-save', JSON.stringify(error));
        return;
      }
    });
    rows = [];
  }

  service.saveTimeoutId = setTimeout(service.saveTradesBuffer, service.timeoutDelay);
  console.log('bitmexSaveTradesBuffer', service.tradeLastTimestamp);
};

service.calculateVolume = () => {
  if (service.calcTimeoutId) {
    clearTimeout(service.calcTimeoutId);
  }

  let sql;
  let timestamp1;
  let timestamp2;
  timestamp1 = new Date(service.tradeLastTimestamp);
  timestamp1.setSeconds(0, 0);
  timestamp2 = new Date(timestamp1.getTime() - 24 * 60 * 60 * 1000);
  timestamp1 = timestamp1.toISOString();
  timestamp2 = timestamp2.toISOString();

  const symbol = 'XBTUSD';
  sql = sprintf("DELETE FROM `%s_%s` WHERE `timestamp` < '%s';", dbTblName.tradesBuffer, symbol, timestamp2);
  dbConn.query(sql, null, (error, result, fields) => {
  });

  timestamp1 = new Date(service.tradeLastTimestamp);
  timestamp1.setSeconds(0, 0);
  timestamp2 = new Date(timestamp1.getTime() - 60 * 1000);
  timestamp1 = timestamp1.toISOString();
  timestamp2 = timestamp2.toISOString();
  sql = sprintf("SELECT IFNULL(SUM(`size`), 0) `volume` FROM `%s_%s` WHERE `timestamp` > '%s' AND `timestamp` <= '%s';", dbTblName.tradesBuffer, symbol, timestamp2, timestamp1);
  const volumeTimestamp1m = timestamp1;
  dbConn.query(sql, null, (error, rows, fields) => {
    if (error) {
      console.error(JSON.stringify(error));
      return;
    }
    const volume = rows[0]['volume'];
    sql = sprintf("INSERT INTO `%s_%s`(`timestamp`, `volume`) VALUES('%s', '%s') ON DUPLICATE KEY UPDATE `volume` = VALUES(`volume`);", dbTblName.volume1m, symbol, volumeTimestamp1m, volume);
    dbConn.query(sql, null, (error, rows, fields) => {
    });
  });

  timestamp1 = new Date(service.tradeLastTimestamp);
  timestamp1.setMinutes(Math.floor(timestamp1.getMinutes() / 5) * 5, 0, 0);
  timestamp2 = new Date(timestamp1.getTime() - 5 * 60 * 1000);
  timestamp1 = timestamp1.toISOString();
  timestamp2 = timestamp2.toISOString();
  sql = sprintf("SELECT IFNULL(SUM(`size`), 0) `volume` FROM `%s_%s` WHERE `timestamp` > '%s' AND `timestamp` <= '%s';", dbTblName.tradesBuffer, symbol, timestamp2, timestamp1);
  const volumeTimestamp5m = timestamp1;
  dbConn.query(sql, null, (error, rows, fields) => {
    if (error) {
      console.error(JSON.stringify(error));
      return;
    }
    const volume = rows[0]['volume'];
    sql = sprintf("INSERT INTO `%s_%s`(`timestamp`, `volume`) VALUES('%s', '%s') ON DUPLICATE KEY UPDATE `volume` = VALUES(`volume`);", dbTblName.volume5m, symbol, volumeTimestamp5m, volume);
    dbConn.query(sql, null, (error, rows, fields) => {
    });
  });

  timestamp1 = new Date(service.tradeLastTimestamp);
  timestamp1.setMinutes(0, 0, 0);
  timestamp2 = new Date(timestamp1.getTime() - 60 * 60 * 1000);
  timestamp1 = timestamp1.toISOString();
  timestamp2 = timestamp2.toISOString();
  sql = sprintf("SELECT IFNULL(SUM(`size`), 0) `volume` FROM `%s_%s` WHERE `timestamp` > '%s' AND `timestamp` <= '%s';", dbTblName.tradesBuffer, symbol, timestamp2, timestamp1);
  const volumeTimestamp1h = timestamp1;
  dbConn.query(sql, null, (error, rows, fields) => {
    if (error) {
      console.error(JSON.stringify(error));
      return;
    }
    const volume = rows[0]['volume'];
    sql = sprintf("INSERT INTO `%s_%s`(`timestamp`, `volume`) VALUES('%s', '%s') ON DUPLICATE KEY UPDATE `volume` = VALUES(`volume`);", dbTblName.volume1h, symbol, volumeTimestamp1h, volume);
    dbConn.query(sql, null, (error, rows, fields) => {
    });
  });
  service.calcTimeoutId = setTimeout(service.calculateVolume, service.timeoutDelay);
  console.log('bitmexCalculateVolume', service.tradeLastTimestamp);
}
;

module.exports = service;
