import dbConn from '../core/dbConn';
import {dbTblName, bitmex} from '../core/config';
import request from 'request';
import {sprintf} from 'sprintf-js';
import step from 'step';
import Q from 'q';

let service = {
    timeoutIds: {},
    timeoutDelay: 1000,
};

service.downloadTradeBucketed = (symbol, binSize, startTime) => {
    const timeoutIdKey = symbol + ':' + binSize;
    if (service.timeoutIds[timeoutIdKey]) {
        clearTimeout(service.timeoutIds[timeoutIdKey]);
    }

    try {
        if (startTime.length === 0) {
            if (binSize === '1m') {
                startTime = '2019-04-25T00:00:00.000Z';
            } else if (binSize === '5m') {
                startTime = '2015-09-25T12:00:00.000Z';
                // startTime = '2016-05-05T04:05:00.000Z';
            } else if (binSize === '1h') {
                startTime = '2015-09-25T12:00:00.000Z';
            }
        }
        startTime = startTime.replace("000Z", "100Z");
        const apiBaseUrl = bitmex.testnet ? bitmex.baseUrlTestnet : bitmex.baseUrlRealnet;
        let url = sprintf('%s%s?binSize=%s&partial=false&symbol=%s&count=%d&reverse=false&startTime=%s', apiBaseUrl, bitmex.pathTradeBucketed, binSize, 'XBTUSD', bitmex.bufferSize, startTime);
        console.log('downloadTradeBucketed', url);

        request(url, {}, function (error, response, body) {
            console.log('downloadTradeBucketed', 'end', url);
            if (error) {
                service.timeoutIds[timeoutIdKey] = setTimeout(service.downloadTradeBucketed, service.timeoutDelay, symbol, binSize, startTime);
                console.error('downloadTradeBucketed-error', binSize, startTime, JSON.stringify(error));
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
                            item.timestamp,
                            item.open,
                            item.high,
                            item.low,
                            item.close,
                            item.volume,
                        ]);
                        lastTimestamp = item.timestamp;
                    }
                    sql = sprintf("INSERT INTO `%s_%s_%s`(`timestamp`, `open`, `high`, `low`, `close`, `volume`) VALUES ? ON DUPLICATE KEY UPDATE `open` = VALUES(`open`), `high` = VALUES(`high`), `low` = VALUES(`low`), `close` = VALUES(`close`), `volume` = VALUES(`volume`);", dbTblName.tradeBucketed, symbol, binSize);

                    console.log('downloadTradeBucketed', 'mysql-start');
                    dbConn.query(sql, [rows], (error, results, fields) => {
                        if (error) {
                            service.timeoutIds[timeoutIdKey] = setTimeout(service.downloadTradeBucketed, service.timeoutDelay, symbol, binSize, startTime);
                            console.error('downloadTradeBucketed-mysql', binSize, startTime, JSON.stringify(error));
                        } else {
                            service.timeoutIds[timeoutIdKey] = setTimeout(service.downloadTradeBucketed, service.timeoutDelay, symbol, binSize, lastTimestamp);
                            console.log('downloadTradeBucketed', binSize, lastTimestamp);
                        }
                    });
                } else {
                    service.timeoutIds[timeoutIdKey] = setTimeout(service.downloadTradeBucketed, service.timeoutDelay, symbol, binSize, startTime);
                    console.log('downloadTradeBucketed', binSize, startTime);
                }
            } else {
                service.timeoutIds[timeoutIdKey] = setTimeout(service.downloadTradeBucketed, service.timeoutDelay, symbol, binSize, startTime);
                console.error('downloadTradeBucketed-response', binSize, startTime, 'response', JSON.stringify(response));
            }
        });
    } catch (e) {
        service.timeoutIds[timeoutIdKey] = setTimeout(service.downloadTradeBucketed, service.timeoutDelay, symbol, binSize, startTime);
        console.error('downloadTradeBucketed-error', binSize, startTime);
    }
};

service.getLastTimestamp = (symbol, binSize, cb) => {
    let sql = sprintf("SELECT `timestamp` FROM `%s_%s_%s` ORDER BY `timestamp` DESC LIMIT 1;", dbTblName.tradeBucketed, symbol, binSize);
    dbConn.query(sql, null, (error, rows, fields) => {
        if (error || rows.length === 0) {
            cb(symbol, binSize, '');
            return;
        } else {
            cb(symbol, binSize, rows[0]['timestamp']);
        }
    });
};

module.exports = service;
