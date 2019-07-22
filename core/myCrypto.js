import crypto from 'crypto';
import config from './config';

module.exports = {
    hmacHex : (plain) => {
        const cipher = crypto.createHmac('sha256', config.server.secret).update(plain).digest('hex');
        // console.log('signMessage', plain, cipher);
        return cipher;
    }
};
