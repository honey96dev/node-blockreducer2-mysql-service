import {dbTblName, bitfinex} from '../core/config';
import dbConn from '../core/dbConn';
import {sprintf} from 'sprintf-js';
import WebSocket from 'ws-reconnect';

let service = {
  socket: undefined,
  subscribes: [
    JSON.stringify({
      "event": "subscribe",
      "channel": "trades",
      "symbol": "tETHUSD",
    }),
    JSON.stringify({
      "event": "subscribe",
      "channel": "trades",
      "symbol": "tEOSUSD",
    }),
    JSON.stringify({
      "event": "subscribe",
      "channel": "trades",
      "symbol": "tEOSUSD",
    }),
  ],
};

service.init = () => {
  const wsUrl = bitfinex.wsUrlPublic;
  service.socket = new WebSocket(wsUrl, {
    retryCount: 2, // default is 2
    reconnectInterval: 1 // default is 5
  });
  service.socket.on('connect', () => {
    for (let subscribe of service.subscribes) {
      service.socket.send(subscribe);
    }
  });
  service.socket.on('message', (data) => {
    console.error('bitfinex-ws', data);
    service.socketLastTimestamp = new Date().getTime();
    data = JSON.parse(data);


    // service.onWsMessage(data);
    //
    // if (!!data.request) {
    //   console.log('volumeService', 'message', JSON.stringify(data));
    //   // if (!!data.request.op) {
    //   // }
    // }
    // if (!!data.table) {
    //   const table = data.table;
    //   if (table === 'trade') {
    //     service.onWsTrade(data.action, data.data);
    //   }
    // }
  });

  service.socket.on('reconnect', (data) => {
    const timestamp = new Date().toISOString();
    console.error('volumeService', 'reconnect', timestamp);
  });

  service.socket.on('destroyed', (data) => {
    console.error('volumeService', 'destroyed', timestamp);
  });

  service.socket.start();
};

module.exports = service;
