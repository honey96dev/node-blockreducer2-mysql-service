import cluster from 'cluster';
import bitmexTradeBucketService from '../service/bitmexTradeBucketService';
import bitmexTradeBucketService1 from '../service/bitmexTradeBucketService1';
import bitfinexCandleTradeService from '../service/bitfinexCandleTradeService';
import bitfinexCandleTradeService1 from '../service/bitfinexCandleTradeService1';
import bitmexVolumeService from '../service/bitmexVolumeService';
import bitfinexVolumeService from '../service/bitfinexVolumeService';
import fftService from '../service/fftService';
import fftService1 from '../service/fftService1';
import fftFixService from '../service/fftFixService';
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
    // //volume
    // setTimeout(bitmexVolumeService.calculateVolume, 20000);
    // setTimeout(bitfinexVolumeService.calculateVolume, 20000);

    id0Service.startCalculation('XBTUSD');
    id0Service.startCalculation('tETHUSD');
    id0Service.startCalculation('tBABUSD');
    id0Service.startCalculation('tEOSUSD');
    id0Service.startCalculation('tLTCUSD');
    id0Service.startCalculation('tBSVUSD');
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
    setTimeout(fftService.getLastTimestamp, 105000, 'tBSVUSD', '5m', (symbol, binSize, timestamp) => {
        fftService.calculateFFT(symbol, binSize, timestamp);
    });
    setTimeout(fftService.getLastTimestamp, 115000, 'tBSVUSD', '1h', (symbol, binSize, timestamp) => {
        fftService.calculateFFT(symbol, binSize, timestamp);
    });

    fftFixService.checkFixRequest();

}
