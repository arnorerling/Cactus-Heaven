const amqp = require('amqplib/callback_api');
const fs = require('fs');
const db = require('./data/db');
const moment = require('moment');


const messageBrokerInfo = {
    exchanges: {
        order: 'order_exchange'
    },
    queues: {
        orderQueue: "order_queue"
    },
    routingKeys: {
        createOrder: 'create_order'
    }
}

const createMessageBrokerConnection = () => new Promise((resolve, reject) => {
    amqp.connect('amqp://localhost', (err, conn) => {
        if (err) { reject(err); }
        resolve(conn);
    });
});

const configureMessageBroker = channel => {
    const { exchanges, queues, routingKeys } = messageBrokerInfo;

    channel.assertExchange(exchanges.order, 'direct', { durable: true });
    channel.assertQueue(queues.orderQueue, { durable: true });
    channel.bindQueue(queues.orderQueue, exchanges.order, routingKeys.createOrder);
};

const createChannel = connection => new Promise((resolve, reject) => {
    connection.createChannel((err, channel) => {
        if (err) { reject(err); }
        configureMessageBroker(channel);
        resolve(channel);
    });
});

(async () => {
    const connection = await createMessageBrokerConnection();
    const channel = await createChannel(connection);

    const { orderQueue } = messageBrokerInfo.queues;
        
    channel.consume(orderQueue, data => {
        var totalPrice = 0;
        console.log(JSON.parse(data.content.toString()));
        const tmpOrder = JSON.parse(data.content.toString());
        tmpOrder.items.forEach(item => {
            totalPrice += item.quantity * item.unitPrice;
        });
        const newOrder = {
            customerEmail: tmpOrder.email,
            totalPrice: totalPrice,
            orderDate: moment().format('llll')
        };
        db.Order.create(newOrder, (err, order) => {
            tmpOrder.items.forEach(item => {
                const newOrderItem = {
                    description: item.description,
                    quantity: item.quantity,
                    unitPrice: item.unitPrice,
                    rowPrice: item.unitPrice * item.quantity,
                    orderId: order.id
                };
                db.OrderItem.create(newOrderItem);
            });
        })
    }, { noAck: true });
})().catch(e => console.error(e));


// description: { type: String, required: true },
//     quantity: { type: Number, required: true },
//     unitPrice: { type: Number, required: true },
//     rowPrice: { type: Number, required: true },
//     orderId: { type: Schema.ObjectId, required: true }