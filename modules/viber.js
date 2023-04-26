const axios = require('axios');
const config = require('../config.json');
const logger = require('./logger');
const FormData = require('form-data');

const viber = {};

viber.sendMessage = (token, payload) => {
    const options = {
        method : 'post',
        url : config.Viber.Viber_Endpoint ,
        responseType : 'json',
        data : payload,
        headers : {
            'Content-Type': 'application/json',
            'X-Viber-Auth-Token' : token,
        }
    }
    /*
    axios(options).catch(error => {
        return error.response.data;
    }).then(response => {
        return response.data;
    });
    */
    return axios(options);
   
}

module.exports = viber;