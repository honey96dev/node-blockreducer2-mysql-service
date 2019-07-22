import dbConn from '../core/dbConn';
import {dbTblName, bitmex} from '../core/config';
import request from 'request';
import {sprintf} from 'sprintf-js';
import step from 'step';
import Q from 'q';

let service = {
    timeoutIds: {},
    timeoutDelay: 30000,
};

service.downloadTradeBucketed = (binSize, startTime) => {
    if (service.timeoutIds[binSize]) {
        clearTimeout(service.timeoutIds[binSize]);
    }

    // step(
    //     () => {
    //         if (startTime.length === 0) {
    //             if (binSize === '1m') {
    //                 startTime = '2019-04-25T00:00:00.000Z';
    //             } else if (binSize === '5m') {
    //                 startTime = '2015-09-25T12:00:00.000Z';
    //             } else if (binSize === '1h') {
    //                 startTime = '2015-09-25T12:00:00.000Z';
    //             }
    //         }
    //         startTime = startTime.replace("000Z", "100Z");
    //         let url = sprintf('https://www.bitmex.com/api/v1/trade/bucketed?binSize=%s&partial=false&symbol=%s&count=%d&reverse=false&startTime=%s', binSize, 'XBTUSD', 750, startTime);
    //         console.log('downloadTradeBucketed', url);
    //
    //         return request(url, {}, this);
    //     },
    //     (error, response, body) => {
    //         if (error) {
    //             service.timeoutIds[binSize] = setTimeout(service.downloadTradeBucketed, service.timeoutDelay, binSize, startTime);
    //             console.error('downloadTradeBucketed-error', binSize, startTime, JSON.stringify(error), JSON.stringify(body));
    //             // this.done();
    //         }
    //
    //         if (!response || response.statusCode !== 200) {
    //             service.timeoutIds[binSize] = setTimeout(service.downloadTradeBucketed, service.timeoutDelay, binSize, startTime);
    //             console.error('downloadTradeBucketed-error response', binSize, startTime, !!response ? response.statusCode : -1, JSON.stringify(response));
    //             // this.done();
    //         }
    //
    //         if (!response || response.statusCode !== 200) {
    //             let items = JSON.parse(body);
    //             if (items.length > 0) {
    //                 let sql;
    //                 let lastTimestamp;
    //                 let rows = [];
    //                 for (let item of items) {
    //                     rows.push([
    //                         item.timestamp,
    //                         item.symbol,
    //                         item.open,
    //                         item.high,
    //                         item.low,
    //                         item.close,
    //                         item.volume,
    //                     ]);
    //                     lastTimestamp = item.timestamp;
    //                 }
    //                 sql = sprintf("INSERT INTO `%s_%s`(`timestamp`, `symbol`, `open`, `high`, `low`, `close`, `volume`) VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `symbol` = VALUES(`symbol`), `open` = VALUES(`open`), `high` = VALUES(`high`), `low` = VALUES(`low`), `close` = VALUES(`close`), `volume` = VALUES(`volume`);", dbTblName.tradeBucketed1h, binSize);
    //
    //                 return dbConn.query(sql, [rows], this);
    //             }
    //         }
    //     },
    //     (error, results, fields) => {
    //         if (error) {
    //             service.timeoutIds[binSize] = setTimeout(service.downloadTradeBucketed, service.timeoutDelay, binSize, startTime);
    //             console.error('downloadTradeBucketed-error mysql', binSize, startTime, JSON.stringify(error));
    //         } else {
    //             service.timeoutIds[binSize] = setTimeout(service.downloadTradeBucketed, service.timeoutDelay, binSize, startTime);
    //             console.log('downloadTradeBucketed', binSize, startTime);
    //         }
    //     }
    // );


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
        const apiBaseUrl = bitmex.testnet ? bitmex.testnetApi : bitmex.realnetApi;
        let url = sprintf('%s/trade/bucketed?binSize=%s&partial=false&symbol=%s&count=%d&reverse=false&startTime=%s', apiBaseUrl, binSize, 'XBTUSD', 750, startTime);
        console.log('downloadTradeBucketed', url);

        request(url, {}, function (error, response, body) {
            if (error) {
                service.timeoutIds[binSize] = setTimeout(service.downloadTradeBucketed, service.timeoutDelay, binSize, startTime);
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
                            item.symbol,
                            item.open,
                            item.high,
                            item.low,
                            item.close,
                            item.volume,
                        ]);
                        lastTimestamp = item.timestamp;
                    }
                    sql = sprintf("INSERT INTO `%s_%s`(`timestamp`, `symbol`, `open`, `high`, `low`, `close`, `volume`) VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `symbol` = VALUES(`symbol`), `open` = VALUES(`open`), `high` = VALUES(`high`), `low` = VALUES(`low`), `close` = VALUES(`close`), `volume` = VALUES(`volume`);", dbTblName.tradeBucketed, binSize);

                    console.log('downloadTradeBucketed', 'mysql-start');
                    dbConn.query(sql, [rows], (error, results, fields) => {
                        if (error) {
                            service.timeoutIds[binSize] = setTimeout(service.downloadTradeBucketed, service.timeoutDelay, binSize, startTime);
                            console.error('downloadTradeBucketed-mysql', binSize, startTime, JSON.stringify(error));
                        } else {
                            service.timeoutIds[binSize] = setTimeout(service.downloadTradeBucketed, service.timeoutDelay, binSize, lastTimestamp);
                            console.log('downloadTradeBucketed', binSize, lastTimestamp);
                        }
                    });
                } else {
                    service.timeoutIds[binSize] = setTimeout(service.downloadTradeBucketed, service.timeoutDelay, binSize, startTime);
                    console.log('downloadTradeBucketed', binSize, startTime);
                }
            } else {
                service.timeoutIds[binSize] = setTimeout(service.downloadTradeBucketed, service.timeoutDelay, binSize, startTime);
                console.error('downloadTradeBucketed-response', binSize, startTime, 'response', JSON.stringify(response));
            }
        });
    } catch (e) {
        service.timeoutIds[binSize] = setTimeout(service.downloadTradeBucketed, service.timeoutDelay, binSize, startTime);
        console.error('downloadTradeBucketed-error', binSize, startTime);
    }
};

service.getLastTimestamp = (binSize, cb) => {
    let sql = sprintf("SELECT `timestamp` FROM `%s_%s` ORDER BY `timestamp` DESC LIMIT 1;", dbTblName.tradeBucketed, binSize);
    dbConn.query(sql, null, (error, rows, fields) => {
        if (error || rows.length === 0) {
            cb(binSize, '');
            return;
        } else {
            cb(binSize, rows[0]['timestamp']);
        }
    });
};

module.exports = service;
