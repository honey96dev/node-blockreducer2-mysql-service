import cluster from 'cluster';
import bitmexTradeBucketService from '../service/bitmexTradeBucketService';
import bitmexTradeBucketService1 from '../service/bitmexTradeBucketService1';
import bitfinexCandleTradeService from '../service/bitfinexCandleTradeService';
import bitfinexCandleTradeService1 from '../service/bitfinexCandleTradeService1';
import bitmexVolumeService from '../service/bitmexVolumeService';
import bitfinexVolumeService from '../service/bitfinexVolumeService';
import fftService from '../service/fftService';
import fftService1 from '../service/fftService1';
import bitmexInstrumentService from '../service/bitmexInstrumentService';
import deribitInstrumentService from '../service/deribitInstrumentService';
import id0Service from '../service/id0Service';
import id0Service1 from '../service/id0Service1';

if (cluster.isMaster) {
    cluster.fork();
    cluster.on('exit', function (worker, code, signal) {
        cluster.fork();
    });
}

if (cluster.isWorker) {
    //trades bucket
    setTimeout(bitmexTradeBucketService.getLastTimestamp, 0, 'XBTUSD', '1m', (symbol, binSize, timestamp) => {
        bitmexTradeBucketService.downloadTradeBucketed(symbol, binSize, timestamp);
    });
    setTimeout(bitmexTradeBucketService.getLastTimestamp, 3000, 'XBTUSD', '5m', (symbol, binSize, timestamp) => {
        bitmexTradeBucketService.downloadTradeBucketed(symbol, binSize, timestamp);
    });
    setTimeout(bitmexTradeBucketService.getLastTimestamp, 6000, 'XBTUSD', '1h', (symbol, binSize, timestamp) => {
        bitmexTradeBucketService.downloadTradeBucketed(symbol, binSize, timestamp);
    });
    setTimeout(bitfinexCandleTradeService.getLastTimestamp, 0, 'tETHUSD', '1m', (symbol, binSize, timestamp) => {
        bitfinexCandleTradeService.downloadCandleTrade(symbol, binSize, timestamp);
    });
    setTimeout(bitfinexCandleTradeService.getLastTimestamp, 3500, 'tETHUSD', '5m', (symbol, binSize, timestamp) => {
        bitfinexCandleTradeService.downloadCandleTrade(symbol, binSize, timestamp);
    });
    setTimeout(bitfinexCandleTradeService.getLastTimestamp, 6500, 'tETHUSD', '1h', (symbol, binSize, timestamp) => {
        bitfinexCandleTradeService.downloadCandleTrade(symbol, binSize, timestamp);
    });
    setTimeout(bitfinexCandleTradeService.getLastTimestamp, 9500, 'tBABUSD', '1m', (symbol, binSize, timestamp) => {
        bitfinexCandleTradeService.downloadCandleTrade(symbol, binSize, timestamp);
    });
    setTimeout(bitfinexCandleTradeService.getLastTimestamp, 12500, 'tBABUSD', '5m', (symbol, binSize, timestamp) => {
        bitfinexCandleTradeService.downloadCandleTrade(symbol, binSize, timestamp);
    });
    setTimeout(bitfinexCandleTradeService.getLastTimestamp, 15500, 'tBABUSD', '1h', (symbol, binSize, timestamp) => {
        bitfinexCandleTradeService.downloadCandleTrade(symbol, binSize, timestamp);
    });
    setTimeout(bitfinexCandleTradeService.getLastTimestamp, 18500, 'tEOSUSD', '1m', (symbol, binSize, timestamp) => {
        bitfinexCandleTradeService.downloadCandleTrade(symbol, binSize, timestamp);
    });
    setTimeout(bitfinexCandleTradeService.getLastTimestamp, 21500, 'tEOSUSD', '5m', (symbol, binSize, timestamp) => {
        bitfinexCandleTradeService.downloadCandleTrade(symbol, binSize, timestamp);
    });
    setTimeout(bitfinexCandleTradeService.getLastTimestamp, 24500, 'tEOSUSD', '1h', (symbol, binSize, timestamp) => {
        bitfinexCandleTradeService.downloadCandleTrade(symbol, binSize, timestamp);
    });
    setTimeout(bitfinexCandleTradeService.getLastTimestamp, 2500, 'tLTCUSD', '1m', (symbol, binSize, timestamp) => {
        bitfinexCandleTradeService.downloadCandleTrade(symbol, binSize, timestamp);
    });
    setTimeout(bitfinexCandleTradeService.getLastTimestamp, 8500, 'tLTCUSD', '5m', (symbol, binSize, timestamp) => {
        bitfinexCandleTradeService.downloadCandleTrade(symbol, binSize, timestamp);
    });
    setTimeout(bitfinexCandleTradeService.getLastTimestamp, 10500, 'tLTCUSD', '1h', (symbol, binSize, timestamp) => {
        bitfinexCandleTradeService.downloadCandleTrade(symbol, binSize, timestamp);
    });

    setTimeout(bitfinexCandleTradeService1.getLastTimestamp, 12500, 'tBSVUSD', '1m', (symbol, binSize, timestamp) => {
        bitfinexCandleTradeService1.downloadCandleTrade(symbol, binSize, timestamp);
    });
    setTimeout(bitfinexCandleTradeService1.getLastTimestamp, 14500, 'tBSVUSD', '5m', (symbol, binSize, timestamp) => {
        bitfinexCandleTradeService1.downloadCandleTrade(symbol, binSize, timestamp);
    });
    setTimeout(bitfinexCandleTradeService1.getLastTimestamp, 16500, 'tBSVUSD', '1h', (symbol, binSize, timestamp) => {
        bitfinexCandleTradeService1.downloadCandleTrade(symbol, binSize, timestamp);
    });


    //volume
    bitmexVolumeService.startRead([
        'trade:XBTUSD',
    ]);
    setTimeout(bitmexVolumeService.saveTradesBuffer, 10000);
    setTimeout(bitmexVolumeService.calculateVolume, 20000);

    bitfinexVolumeService.startRead([
      {
          event: "subscribe",
          channel: "trades",
          symbol: "tETHUSD",
      },
      {
          event: "subscribe",
          channel: "trades",
          symbol: "tBABUSD",
      },
      {
          event: "subscribe",
          channel: "trades",
          symbol: "tEOSUSD",
      },
      {
          event: "subscribe",
          channel: "trades",
          symbol: "tLTCUSD",
      },
      {
          event: "subscribe",
          channel: "trades",
          symbol: "tBSVUSD",
      },
    ]);
    setTimeout(bitfinexVolumeService.saveTradesBuffer, 10000);
    setTimeout(bitfinexVolumeService.calculateVolume, 20000);


    id0Service.startCalculation('XBTUSD');
    id0Service.startCalculation('tETHUSD');
    id0Service.startCalculation('tBABUSD');
    id0Service.startCalculation('tEOSUSD');
    id0Service.startCalculation('tLTCUSD');
    // id0Service1.startCalculation('tBSVUSD');

    // //fft
    setTimeout(fftService.getLastTimestamp, 5000, 'XBTUSD', '5m', (symbol, binSize, timestamp) => {
        fftService.calculateFFT(symbol, binSize, timestamp);
    });
    setTimeout(fftService.getLastTimestamp, 15000, 'XBTUSD', '1h', (symbol, binSize, timestamp) => {
        fftService.calculateFFT(symbol, binSize, timestamp);
    });
    setTimeout(fftService.getLastTimestamp, 25000, 'tETHUSD', '5m', (symbol, binSize, timestamp) => {
        fftService.calculateFFT(symbol, binSize, timestamp);
    });
    setTimeout(fftService.getLastTimestamp, 35000, 'tETHUSD', '1h', (symbol, binSize, timestamp) => {
        fftService.calculateFFT(symbol, binSize, timestamp);
    });
    setTimeout(fftService.getLastTimestamp, 45000, 'tBABUSD', '5m', (symbol, binSize, timestamp) => {
        fftService.calculateFFT(symbol, binSize, timestamp);
    });
    setTimeout(fftService.getLastTimestamp, 55000, 'tBABUSD', '1h', (symbol, binSize, timestamp) => {
        fftService.calculateFFT(symbol, binSize, timestamp);
    });
    setTimeout(fftService.getLastTimestamp, 65000, 'tEOSUSD', '5m', (symbol, binSize, timestamp) => {
        fftService.calculateFFT(symbol, binSize, timestamp);
    });
    setTimeout(fftService.getLastTimestamp, 75000, 'tEOSUSD', '1h', (symbol, binSize, timestamp) => {
        fftService.calculateFFT(symbol, binSize, timestamp);
    });
    setTimeout(fftService.getLastTimestamp, 85000, 'tLTCUSD', '5m', (symbol, binSize, timestamp) => {
        fftService.calculateFFT(symbol, binSize, timestamp);
    });
    setTimeout(fftService.getLastTimestamp, 95000, 'tLTCUSD', '1h', (symbol, binSize, timestamp) => {
        fftService.calculateFFT(symbol, binSize, timestamp);
    });
    // setTimeout(fftService.getLastTimestamp, 105000, 'tBSVUSD', '5m', (symbol, binSize, timestamp) => {
    //     fftService.calculateFFT(symbol, binSize, timestamp);
    // });
    // setTimeout(fftService.getLastTimestamp, 115000, 'tBSVUSD', '1h', (symbol, binSize, timestamp) => {
    //     fftService.calculateFFT(symbol, binSize, timestamp);
    // });
    // setTimeout(fftService.getLastTimestamp, 5000, 'tBABUSD', '5m', (symbol, binSize, timestamp) => {
    //     fftService.calculateFFT(symbol, binSize, timestamp);
    // });
    // setTimeout(fftService.getLastTimestamp, 15000, 'tBABUSD', '1h', (symbol, binSize, timestamp) => {
    //     fftService.calculateFFT(symbol, binSize, timestamp);
    // });

    // // fft-hist
    // setTimeout(fftService1.getLastTimestamp, 5000, 'tBABUSD', '5m', (symbol, binSize, timestamp) => {
    //     fftService1.calculateFFT(symbol, binSize, timestamp);
    // });
    // setTimeout(fftService1.getLastTimestamp, 15000, 'tBABUSD', '1h', (symbol, binSize, timestamp) => {
    //     fftService1.calculateFFT(symbol, binSize, timestamp);
    // });

    //bitmex instruments
    bitmexInstrumentService.downloadInstrument();

    //deribit instruments
    deribitInstrumentService.downloadDeribitInstruments();
}
