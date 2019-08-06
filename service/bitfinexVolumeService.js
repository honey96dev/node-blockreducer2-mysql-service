import {dbTblName, bitfinex} from '../core/config';
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

  subscribeMap: new Map(),
  tradeBuffer: {
    tETHUSD: new Map(),
    tBABUSD: new Map(),
    tEOSUSD: new Map(),
    tLTCUSD: new Map(),
    // tETHUSD: new Map(),
  },
  tradeLastTimestamp: new Date(1970, 1, 1).toISOString(),
};

service.renewSocket = () => {
  const timestamp = new Date().getTime();
  if (service.socketTimeoutId) {
    clearTimeout(service.socketTimeoutId);
  }
  service.socketTimeoutId = setTimeout(service.renewSocket, service.timeoutDelay);
  if (service.socketLastTimestamp > timestamp - service.timeoutDelay) {
    console.log('bitfinexVolumeService', 'renewSocket-still alive', service.socketLastTimestamp);
    return;
  }

  const wsUrl = bitfinex.wsUrlPublic;
  service.socket = new WebSocket(wsUrl, {
    retryCount: 2, // default is 2
    reconnectInterval: 1 // default is 5
  });
  console.error('bitfinexVolumeService', 'renewSocket', service.socketLastTimestamp);

  service.socket.on('connect', () => {
    for (let subscribe of service.subscribes) {
      service.socket.send(subscribe);
    }
  });
  service.socket.on('message', (data) => {
    service.socketLastTimestamp = new Date().getTime();
    data = JSON.parse(data);

    service.onWsMessage(data);

    if (data instanceof Array) {
      const cnt = data.length;
      const channel = service.subscribeMap.get(data[0]);
      const payload = (data[1] instanceof Array) ? data[1] : [data[2]];
      if (channel.channel === 'trades') {
        if (payload[0] != null) {
          service.onWsTrade(channel.symbol, payload);
        }
      }
    } else {
      if (!!data.event && data.event === 'subscribed') {
        service.subscribeMap.set(data.chanId, {
          channel: data.channel,
          symbol: data.symbol,
        });
      }
    }
  });

  service.socket.on('reconnect', (data) => {
    const timestamp = new Date().toISOString();
    console.error('bitfinexVolumeService', 'reconnect', timestamp);
  });

  service.socket.on('destroyed', (data) => {
    console.error('bitfinexVolumeService', 'destroyed', timestamp);
  });

  service.socket.start();
};

service.onWsMessage = (data) => {
  // console.log('onWsMessage', JSON.stringify(data));
};

service.onWsTrade = (symbol, data) => {
  console.error('onWsTrade', symbol, JSON.stringify(data));
  for (let trade of data) {
    service.tradeBuffer[symbol].set(trade[0], [
      trade[0],
      new Date(trade[1]).toISOString(),
      trade[2],
      trade[3],
    ]);
  }
};

service.startRead = (subscribes) => {
  service.subscribes = [];
  if (subscribes instanceof Array) {
    let query;
    for (let subscribe of subscribes) {
      query = JSON.stringify(subscribe);
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
  let symbols = ['tETHUSD', 'tBABUSD', 'tEOSUSD', 'tLTCUSD'];
  // console.log(service.tradeBuffer);
  for (let symbol of symbols) {
    // console.log('tETHUSD', service.tradeBuffer[symbol]);
    service.tradeBuffer[symbol].forEach((value, key, map) => {
      // console.log(key, value)
      rows.push(value);
      if (service.tradeLastTimestamp < value[1]) {
        service.tradeLastTimestamp = value[1];
      }

      if (rows.length > 512) {
        sql = sprintf("INSERT INTO `%s_%s`(`id`, `timestamp`, `size`, `price`) VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `size` = VALUES(`size`), `price` = VALUES(`price`);", dbTblName.tradesBuffer, symbol);
        dbConn.query(sql, [rows], (error, result, fields) => {
          if (error) {
            console.error('bitfinexVolumeService', 'tradesBuffer-save', JSON.stringify(error));
            return;
          }
        });
        rows = [];
      }
    });
    service.tradeBuffer[symbol].clear();
    if (rows.length > 0) {
      sql = sprintf("INSERT INTO `%s_%s`(`id`, `timestamp`, `size`, `price`) VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `size` = VALUES(`size`), `price` = VALUES(`price`);", dbTblName.tradesBuffer, symbol);
      dbConn.query(sql, [rows], (error, result, fields) => {
        if (error) {
          console.error('bitfinexVolumeService', 'tradesBuffer-save', JSON.stringify(error));
          return;
        }
      });
      rows = [];
    }
  }

  service.saveTimeoutId = setTimeout(service.saveTradesBuffer, service.timeoutDelay);
  console.log('bitfinxSaveTradesBuffer', service.tradeLastTimestamp);
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


  let symbols = ['tETHUSD', 'tBABUSD', 'tEOSUSD', 'tLTCUSD'];
  for (let symbol of symbols) {
    sql = sprintf("DELETE FROM `%s_%s` WHERE `timestamp` < '%s';", dbTblName.tradesBuffer, symbol, timestamp2);
    dbConn.query(sql, null, (error, result, fields) => {
    });

    timestamp1 = new Date(service.tradeLastTimestamp);
    timestamp1.setSeconds(0, 0);
    timestamp2 = new Date(timestamp1.getTime() + 60 * 1000);
    timestamp1 = timestamp1.toISOString();
    timestamp2 = timestamp2.toISOString();
    sql = sprintf("SELECT IFNULL(SUM(`price`), 0) `volume` FROM `%s_%s` WHERE `timestamp` > '%s' AND `timestamp` <= '%s';", dbTblName.tradesBuffer, symbol, timestamp1, timestamp2);
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
    timestamp2 = new Date(timestamp1.getTime() + 5 * 60 * 1000);
    timestamp1 = timestamp1.toISOString();
    timestamp2 = timestamp2.toISOString();
    sql = sprintf("SELECT IFNULL(SUM(`price`), 0) `volume` FROM `%s_%s` WHERE `timestamp` > '%s' AND `timestamp` <= '%s';", dbTblName.tradesBuffer, symbol, timestamp1, timestamp2);
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
    timestamp2 = new Date(timestamp1.getTime() + 60 * 60 * 1000);
    timestamp1 = timestamp1.toISOString();
    timestamp2 = timestamp2.toISOString();
    sql = sprintf("SELECT IFNULL(SUM(`price`), 0) `volume` FROM `%s_%s` WHERE `timestamp` > '%s' AND `timestamp` <= '%s';", dbTblName.tradesBuffer, symbol, timestamp1, timestamp2);
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
  }
  service.calcTimeoutId = setTimeout(service.calculateVolume, service.timeoutDelay);
  console.log('bitfinexCalculateVolume', service.tradeLastTimestamp);
};

module.exports = service;
