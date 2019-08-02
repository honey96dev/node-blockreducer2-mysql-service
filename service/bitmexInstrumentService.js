import dbConn from '../core/dbConn';
import {dbTblName, bitmex} from '../core/config';
import request from 'request';
import {sprintf} from 'sprintf-js';
import step from 'step';
import Q from 'q';

let service = {
    timeoutId: undefined,
    timeoutDelay: 3000,
};

service.downloadInstrument = () => {
    if (service.timeoutId) {
        clearTimeout(service.timeoutId);
    }

    try {
        const apiBaseUrl = bitmex.testnet ? bitmex.baseUrlTestnet : bitmex.baseUrlRealnet;
        let url = sprintf('%s%s?symbol=XBTUSD&count=100&reverse=false', apiBaseUrl, bitmex.pathInstrument);
        console.log('downloadInstrument', url);

        request(url, {}, function (error, response, body) {
            if (error) {
                service.timeoutId = setTimeout(service.downloadInstrument);
                console.error('downloadInstrument-error', JSON.stringify(error));
                return;
            }

            if (response && response.statusCode === 200) {
                let items = JSON.parse(body);
                if (items.length > 0) {let sql;
                    let timestamp;
                    let lastTimestamp;
                    let rows = [];
                    let openInterests = [];
                    let item = items[0];
                    {
                        lastTimestamp = item.timestamp;
                        timestamp = new Date(lastTimestamp);
                        timestamp.setMinutes(timestamp.getMinutes(), 0, 0);
                        rows = [
                            timestamp.toISOString(),
                            parseFloat(item.vwap) * 3 / (parseFloat(item.highPrice) + parseFloat(item.lowPrice) + parseFloat(item.lastPrice)),
                        ];
                        openInterests = [
                            timestamp.toISOString(),
                            item.openInterest,
                            item.openValue,
                        ];
                    }
                    sql = sprintf("INSERT INTO `%s`(`timestamp`, `vwap_seed`) VALUES ('%s', '%f') ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `vwap_seed` = VALUES(`vwap_seed`);", dbTblName.vwap1m, rows[0], rows[1]);

                    dbConn.query(sql, null, (error, results, fields) => {
                        if (error) {
                            console.error('downloadInstrument-mysql', JSON.stringify(error));
                        } else {
                            console.log('downloadInstrument-vwap-seed', lastTimestamp);
                        }
                    });

                    sql = sprintf("INSERT INTO `%s`(`timestamp`, `openInterest`, `openValue`) VALUES ('%s', '%f', '%f') ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `openInterest` = VALUES(`openInterest`), `openValue` = VALUES(`openValue`);", dbTblName.interestedNValue1m, openInterests[0], openInterests[1], openInterests[2]);

                    dbConn.query(sql, null, (error, results, fields) => {
                        if (error) {
                            service.timeoutId = setTimeout(service.downloadInstrument, service.timeoutDelay);
                            console.error('downloadInstrument-mysql', JSON.stringify(error));
                        } else {
                            service.timeoutId = setTimeout(service.downloadInstrument, service.timeoutDelay);
                            console.log('downloadInstrument-open-interest', lastTimestamp);
                        }
                    });

                    service.calculateInstruments(lastTimestamp);
                } else {
                    service.timeoutId = setTimeout(service.downloadInstrument, service.timeoutDelay);
                    console.log('downloadInstrument');
                }
            } else {
                service.timeoutId = setTimeout(service.downloadInstrument, service.timeoutDelay);
                console.error('downloadInstrument-response', 'response', JSON.stringify(response));
            }
        });
    } catch (e) {
        service.timeoutId = setTimeout(service.downloadInstrument, service.timeoutDelay);
        console.error('downloadInstrument-error');
    }
};

service.calculateInstruments = (timestamp) => {
    timestamp = new Date(timestamp);
    let minutes = timestamp.getMinutes();
    let sql;

    let timestamp1 = timestamp;
    let timestamp2;
    timestamp1.setMinutes(Math.floor(minutes / 5) * 5, 0, 0);
    timestamp2 = new Date(timestamp1.getTime() + 5 * 60 * 1000);
    timestamp1 = timestamp1.toISOString();
    timestamp2 = timestamp2.toISOString();
    sql = sprintf("SELECT AVG(`vwap_seed`) `vwap_seed` FROM `%s` WHERE `timestamp` >= '%s' AND `timestamp` < '%s';", dbTblName.vwap1m, timestamp1, timestamp2);
    console.log('calculateInstruments', '5m', 'get', sql);
    dbConn.query(sql, null, (error, rows, fields) => {
        if (error) {
            console.error('calculateInstruments', 'vwap', '5m', 'get', JSON.stringify(error));
            return;
        }
        if (rows && rows.length > 0) {
            sql = sprintf("INSERT INTO `%s`(`timestamp`, `vwap_seed`) VALUES('%s', '%s') ON DUPLICATE KEY UPDATE `vwap_seed` = VALUES(`vwap_seed`);", dbTblName.vwap5m, timestamp1, rows[0]['vwap_seed']);
            dbConn.query(sql, null, (error, rows, fields) => {
                if (error) {
                    console.error('calculateInstruments', 'vwap', '5m', 'put', JSON.stringify(error));
                }
            });
        }
    });

    let timestamp3 = timestamp;
    let timestamp4;
    timestamp3.setMinutes(0, 0, 0);
    timestamp4 = new Date(timestamp3.getTime() + 60 * 60 * 1000);
    timestamp3 = timestamp3.toISOString();
    timestamp4 = timestamp4.toISOString();
    sql = sprintf("SELECT AVG(`vwap_seed`) `vwap_seed` FROM `%s` WHERE `timestamp` >= '%s' AND `timestamp` < '%s';", dbTblName.vwap1m, timestamp3, timestamp4);
    console.log('calculateInstruments', '1h', 'get', sql);
    dbConn.query(sql, null, (error, rows, fields) => {
        if (error) {
            console.error('calculateInstruments', 'vwap', '1h', 'get', JSON.stringify(error));
            return;
        }
        if (rows && rows.length > 0) {
            sql = sprintf("INSERT INTO `%s`(`timestamp`, `vwap_seed`) VALUES('%s', '%s') ON DUPLICATE KEY UPDATE `vwap_seed` = VALUES(`vwap_seed`);", dbTblName.vwap1h, timestamp3, rows[0]['vwap_seed']);
            dbConn.query(sql, null, (error, rows, fields) => {
                if (error) {
                    console.error('calculateInstruments', 'vwap', '1h', 'put', JSON.stringify(error));
                }
            });
        }
    });


    sql = sprintf("SELECT AVG(`openInterest`) `openInterest`, AVG(`openValue`) `openValue` FROM `%s` WHERE `timestamp` >= '%s' AND `timestamp` < '%s';", dbTblName.interestedNValue1m, timestamp1, timestamp2);
    console.log('calculateInstruments', 'interested_n_value', '1h', 'get', sql);
    dbConn.query(sql, null, (error, rows, fields) => {
        if (error) {
            console.error('calculateInstruments', 'interested_n_value', '5m', 'get', JSON.stringify(error));
            return;
        }
        if (rows && rows.length > 0) {
            sql = sprintf("INSERT INTO `%s`(`timestamp`, `openInterest`, `openValue`) VALUES ('%s', '%f', '%f') ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `openInterest` = VALUES(`openInterest`), `openValue` = VALUES(`openValue`);", dbTblName.interestedNValue5m, timestamp1, rows[0]['openInterest'], rows[0]['openValue']);
            dbConn.query(sql, null, (error, rows, fields) => {
                if (error) {
                    console.error('calculateInstruments', 'interested_n_value', '5m', 'put', JSON.stringify(error));
                }
            });
        }
    });

    sql = sprintf("SELECT AVG(`openInterest`) `openInterest`, AVG(`openValue`) `openValue` FROM `%s` WHERE `timestamp` >= '%s' AND `timestamp` < '%s';", dbTblName.interestedNValue1m, timestamp3, timestamp4);
    console.log('calculateInstruments', 'interested_n_value', '1h', 'get', sql);
    dbConn.query(sql, null, (error, rows, fields) => {
        if (error) {
            console.error('calculateInstruments', 'interested_n_value', '1h', 'get', JSON.stringify(error));
            return;
        }
        if (rows && rows.length > 0) {
            sql = sprintf("INSERT INTO `%s`(`timestamp`, `openInterest`, `openValue`) VALUES ('%s', '%f', '%f') ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `openInterest` = VALUES(`openInterest`), `openValue` = VALUES(`openValue`);", dbTblName.interestedNValue1h, timestamp3, rows[0]['openInterest'], rows[0]['openValue']);
            dbConn.query(sql, null, (error, rows, fields) => {
                if (error) {
                    console.error('calculateInstruments', 'interested_n_value', '1h', 'put', JSON.stringify(error));
                }
            });
        }
    });
};

module.exports = service;
