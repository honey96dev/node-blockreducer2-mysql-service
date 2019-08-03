import dbConn from '../core/dbConn';
import {dbTblName, bitfinex, bitmex} from '../core/config';
import request from 'request';
import {sprintf} from 'sprintf-js';

let service = {
  timeoutIds: {},
  timeoutDelay: 1000,
};

service.downloadCandleTrade = (symbol, timeframe, startTime) => {
  const timeoutIdKey = symbol + ':' + timeframe;
  if (service.timeoutIds[timeoutIdKey]) {
    clearTimeout(service.timeoutIds[timeoutIdKey]);
  }

  try {
    if (startTime.length === 0) {
      if (timeframe === '1m') {
        startTime = 1364774819000;
      } else if (timeframe === '5m') {
        startTime = 1364774699000;
      } else if (timeframe === '1h') {
        startTime = 1364770799000;
      }
    }
    // startTime++;

    const apiBaseUrl = bitfinex.baseUrlRealnet;
    let url = sprintf('%s%s:%s:%s/hist?start=%d&sort=1&limit=%d', apiBaseUrl, bitfinex.pathCandleTrade, timeframe, symbol, startTime, bitfinex.bufferSize);
    console.log('downloadCandleTrade', url);
    request(url, {timeout: 5000}, function (error, response, body) {
      console.log('downloadCandleTrade', 'request-end');
      if (error) {
        service.timeoutIds[timeoutIdKey] = setTimeout(service.downloadCandleTrade, service.timeoutDelay, symbol, timeframe, startTime);
        console.error('downloadCandleTrade-error1', timeframe, startTime, JSON.stringify(error));
        return;
      }

      if (response && response.statusCode === 200) {
        let items = JSON.parse(body);
        if (items.length > 0) {
          let sql;
          let lastTimestamp;
          let rows = [];
          for (let item of items) {
            rows.push([
              new Date(item[0]).toISOString(),
              item[1],
              item[2],
              item[3],
              item[4],
              item[5],
            ]);
            lastTimestamp = item[0];
          }
          sql = sprintf("INSERT INTO `%s_%s_%s`(`timestamp`, `open`, `high`, `low`, `close`, `volume`) VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `open` = VALUES(`open`), `high` = VALUES(`high`), `low` = VALUES(`low`), `close` = VALUES(`close`), `volume` = VALUES(`volume`);", dbTblName.tradeBucketed, symbol, timeframe);

          console.log('downloadCandleTrade', 'mysql-start');
          dbConn.query(sql, [rows], (error, results, fields) => {
            if (error) {
              service.timeoutIds[timeoutIdKey] = setTimeout(service.downloadCandleTrade, service.timeoutDelay, symbol, timeframe, startTime);
              console.error('downloadCandleTrade-mysql', timeframe, startTime, JSON.stringify(error));
            } else {
              service.timeoutIds[timeoutIdKey] = setTimeout(service.downloadCandleTrade, service.timeoutDelay, symbol, timeframe, lastTimestamp);
              console.log('downloadCandleTrade', timeframe, lastTimestamp);
            }
          });
        } else {
          service.timeoutIds[timeoutIdKey] = setTimeout(service.downloadCandleTrade, service.timeoutDelay, symbol, timeframe, startTime);
          console.log('downloadCandleTrade', timeframe, startTime);
        }
      } else {
        service.timeoutIds[timeoutIdKey] = setTimeout(service.downloadCandleTrade, service.timeoutDelay, symbol, timeframe, startTime);
        console.error('downloadCandleTrade-response', timeframe, startTime, 'response', JSON.stringify(response));
      }
    });
  } catch (e) {
    service.timeoutIds[timeoutIdKey] = setTimeout(service.downloadCandleTrade, service.timeoutDelay, symbol, timeframe, startTime);
    console.error('downloadCandleTrade-error2', timeframe, startTime);
  }
};

service.getLastTimestamp = (symbol, timeframe, cb) => {
  let sql = sprintf("SELECT `timestamp` FROM `%s_%s_%s` ORDER BY `timestamp` DESC LIMIT 1;", dbTblName.tradeBucketed, symbol, timeframe);
  dbConn.query(sql, null, (error, rows, fields) => {
    if (error || rows.length === 0) {
      cb(symbol, timeframe, 0);
    } else {
      cb(symbol, timeframe, new Date(rows[0]['timestamp']).getTime());
    }
  });
};

module.exports = service;
