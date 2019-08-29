'use strict';

var kafka = require('kafka-node');
var Consumer = kafka.Consumer;
var Offset = kafka.Offset;
var Client = kafka.KafkaClient;
var argv = require('optimist').argv;
var topic = argv.topic || 'swc-uds-pingd-test-access-log-json';

var client = new Client({
    kafaHost: '10.125.253.112:9092,10.125.253.68:9092,10.125.253.37:9092,10.125.252.83:9092,10.125.252.114:9092', connectTimeout: 1000, sasl: {
        mechanism: 'plain', username: 'lSE10Z8BWLdB', password: 'w0GqGhOoD9G6'
    }

});
var topics = [{ topic: topic, partition: 1 }, { topic: topic, partition: 0 }];
var options = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 };

var consumer = new Consumer(client, topics, options);
var offset = new Offset(client);

consumer.on('message', function (message) {
    console.log(message);
});

consumer.on('error', function (err) {
    console.log('error', err);
});

/*
* If consumer get `offsetOutOfRange` event, fetch data from the smallest(oldest) offset
*/
consumer.on('offsetOutOfRange', function (topic) {
    topic.maxNum = 2;
    offset.fetch([topic], function (err, offsets) {
        if (err) {
            return console.error(err);
        }
        var min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
        consumer.setOffset(topic.topic, topic.partition, min);
    });
});