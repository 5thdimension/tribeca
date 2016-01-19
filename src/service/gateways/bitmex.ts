/// <reference path="../../../typings/tsd.d.ts" />
/// <reference path="../utils.ts" />
/// <reference path="../../common/models.ts" />
/// <reference path="nullgw.ts" />
///<reference path="../interfaces.ts"/>

import ws = require('ws');
import Q = require("q");
import crypto = require("crypto");
import request = require("request");
import url = require("url");
import querystring = require("querystring");
import Config = require("../config");
import NullGateway = require("./nullgw");
import Models = require("../../common/models");
import Utils = require("../utils");
import util = require("util");
import Interfaces = require("../interfaces");
import moment = require("moment");
import _ = require("lodash");
var shortId = require("shortid");

interface BitMEXMessageIncomingMessage {
    // Table name / Subscription topic.
    // Could be "trade", "order", etc.
    table : string;
    // The type of the message. Types:
    // 'partial'; This is a table image, replace your data entirely.
    // 'update': Update a single row.
    // 'insert': Insert a new row.
    // 'delete': Delete a row.
    action : string;
    // Attribute names that are guaranteed to be unique per object.
    // If more than one is provided, the key is composite.
    // Use these key names to uniquely identify rows. Key columns are guaranteed
    // to be present on all data received.
    keys : string[];
    // An array of objects is emitted here. They are identical in structure to data returned from the REST API.
    data : Object[];
}

interface BitMEXOrder {
  // Client Order ID - this will come back on any execution messages tied to this order.
  clOrdID: ?string;
  // Client Order Link ID
  clOrdLinkID: ?string;
  // Account ID
  account: number;
  // Instrument symbol
  symbol: string;
  // Side e.g. Buy or Sell. If specified as 'Sell', can use positive numbers in qty for sell.
  side: ?string;
  // Quantity of underlying. Use positive numbers to buy, negative to sell.
  simpleOrderQty: ?number;
  // Quantity of contracts. Use positive numbers to buy, negative to sell.
  orderQty: ?number;
  // Limit price for Limit or StopLimit orders.
  price: ?number;
  // Quantity to display in the book.
  displayQty: ?number;
  // Trigger price for Stop or StopLimit orders.
  stopPx: ?number;
  // Trailing offset from mark price for TrailingStopPeg orders.
  pegOffsetValue: ?number;
  // Peg price type e.g. TrailingStopPeg.
  pegPriceType: ?string;
  // Optional order type e.g. Market, Limit, Stop, StopLimit.
  ordType: ?string;
  // Time in force e.g. Day, GoodTillCancel, ImmediateOrCancel, FillOrKill.
  timeInForce: ?string;
  // Execution instruction e.g. ParticipateDoNotInitiate, TrailingStopPeg.
  execInst: ?string;
  // Contingency type e.g. OneCancelsTheOther, OneTriggersTheOther.
  contingencyType: ?string;
  // Optional text
  text: ?string;
};

interface BitMEXDepthMessage {
    asks : [number, number][];
    bids : [number, number][];
    timestamp : string;
}

interface OrderAck {
    result: string; // "true" or "false"
    order_id: number;
}

interface SignedMessage {
    api_key?: string;
    sign?: string;
}

interface Order extends SignedMessage {
    symbol: string;
    type: string;
    price: string;
    amount: string;
}

interface Cancel extends SignedMessage {
    order_id: string;
    symbol: string;
}

interface BitMEXTradeRecord {
    averagePrice: string;
    completedTradeAmount: string;
    createdDate: string;
    id: string;
    orderId: string;
    sigTradeAmount: string;
    sigTradePrice: string;
    status: number;
    symbol: string;
    tradeAmount: string;
    tradePrice: string;
    tradeType: string;
    tradeUnitPrice: string;
    unTrade: string;
}

interface SubscriptionRequest extends SignedMessage { }

class BitMEXWebsocket {
  send = <T>(channel : string, parameters: any) => {
        var subsReq : any = {event: 'addChannel', channel: channel};

        if (parameters !== null)
            subsReq.parameters = parameters;

        this._ws.send(JSON.stringify(subsReq));
    }

    setHandler = <T>(channel : string, handler: (newMsg : Models.Timestamped<T>) => void) => {
        this._handlers[channel] = handler;
    }

    private onMessage = (raw : string) => {
        var t = Utils.date();
        try {
            var msg : BitMEXMessageIncomingMessage = JSON.parse(raw);

            if (typeof msg.event !== "undefined" && msg.event == "ping") {
                this._ws.send(this._serializedHeartbeat);
                return;
            }

            if (typeof msg.success !== "undefined") {
                if (msg.success !== "true")
                    this._log("Unsuccessful message %o", msg);
                else
                    this._log("Successfully connected to %s", msg.channel);
                return;
            }

            var handler = this._handlers[msg.channel];

            if (typeof handler === "undefined") {
                this._log("Got message on unknown topic %o", msg);
                return;
            }

            handler(new Models.Timestamped(msg.data, t));
        }
        catch (e) {
            this._log("Error parsing msg %o", raw);
            throw e;
        }
    };

    ConnectChanged = new Utils.Evt<Models.ConnectivityStatus>();
    private _serializedHeartbeat = JSON.stringify({event: "pong"});
    private _log : Utils.Logger = Utils.log("tribeca:gateway:BitMEXWebsocket");
    private _handlers : { [channel : string] : (newMsg : Models.Timestamped<any>) => void} = {};
    private _ws : ws;
    constructor(config : Config.IConfigProvider) {
        this._ws = new ws(config.GetString("BitMEXWsUrl"));

        this._ws.on("open", () => this.ConnectChanged.trigger(Models.ConnectivityStatus.Connected));
        this._ws.on("message", this.onMessage);
        this._ws.on("close", () => this.ConnectChanged.trigger(Models.ConnectivityStatus.Disconnected));
    }
}

class BitMEXMarketDataGateway implements Interfaces.IMarketDataGateway {
    ConnectChanged = new Utils.Evt<Models.ConnectivityStatus>();

    MarketTrade = new Utils.Evt<Models.GatewayMarketTrade>();
    private onTrade = (trades : Models.Timestamped<[string,string,string,string,string][]>) => {
        // [tid, price, amount, time, type]
        _.forEach(trades.data, trade => {
            var px = parseFloat(trade[1]);
            var amt = parseFloat(trade[2]);
            var side = trade[4] === "ask" ? Models.Side.Ask : Models.Side.Bid; // is this the make side?
            var mt = new Models.GatewayMarketTrade(px, amt, trades.time, trades.data.length > 0, side);
            this.MarketTrade.trigger(mt);
        });
    };

    MarketData = new Utils.Evt<Models.Market>();

    private static GetLevel = (n: [number, number]) : Models.MarketSide =>
        new Models.MarketSide(n[0], n[1]);

    private onDepth = (depth : Models.Timestamped<BitMEXDepthMessage>) => {
        var msg = depth.data;

        var bids = _(msg.bids).first(3).map(BitMEXMarketDataGateway.GetLevel).value();
        var asks = _(msg.asks).reverse().first(3).map(BitMEXMarketDataGateway.GetLevel).value()
        var mkt = new Models.Market(bids, asks, depth.time);

        this.MarketData.trigger(mkt);
    };

    private _log : Utils.Logger = Utils.log("tribeca:gateway:BitMEXMD");
    constructor(socket : BitMEXWebsocket, symbolProvider: BitMEXSymbolProvider) {
        var depthChannel = "ok_" + symbolProvider.symbolWithoutUnderscore + "_depth";
        var tradesChannel = "ok_" + symbolProvider.symbolWithoutUnderscore + "_trades_v1";

        socket.setHandler(depthChannel, this.onDepth);
        socket.setHandler(tradesChannel, this.onTrade);

        socket.ConnectChanged.on(cs => {
            this.ConnectChanged.trigger(cs);

            if (cs == Models.ConnectivityStatus.Connected) {
                socket.send(depthChannel, {});
                socket.send(tradesChannel, {});
            }
        });
    }
}

class BitMEXOrderEntryGateway implements Interfaces.IOrderEntryGateway {
    OrderUpdate = new Utils.Evt<Models.OrderStatusReport>();
    ConnectChanged = new Utils.Evt<Models.ConnectivityStatus>();

    generateClientOrderId = () => shortId.generate();

    public cancelsByClientOrderId = false;

    private static GetOrderType(side: Models.Side, type: Models.OrderType) : string {
        if (side === Models.Side.Bid) {
            if (type === Models.OrderType.Limit) return "buy";
            if (type === Models.OrderType.Market) return "buy_market";
        }
        if (side === Models.Side.Ask) {
            if (type === Models.OrderType.Limit) return "sell";
            if (type === Models.OrderType.Market) return "sell_market";
        }
        throw new Error("unable to convert " + Models.Side[side] + " and " + Models.OrderType[type]);
    }

    // let's really hope there's no race conditions on their end -- we're assuming here that orders sent first
    // will be acked first, so we can match up orders and their acks
    private _ordersWaitingForAckQueue = [];

    sendOrder = (order : Models.BrokeredOrder) : Models.OrderGatewayActionReport => {
        var o : Order = {
            symbol: this._symbolProvider.symbol,
            type: BitMEXOrderEntryGateway.GetOrderType(order.side, order.type),
            price: order.price.toString(),
            amount: order.quantity.toString()};

        this._ordersWaitingForAckQueue.push(order.orderId);

        this._socket.send<OrderAck>("ok_spotusd_trade", this._signer.signMessage(o));
        return new Models.OrderGatewayActionReport(Utils.date());
    };

    private onOrderAck = (ts: Models.Timestamped<OrderAck>) => {
        var orderId = this._ordersWaitingForAckQueue.shift();
        if (typeof orderId === "undefined") {
            this._log("ERROR: got an order ack when there was no order queued!", util.format(ts.data));
            return;
        }

        var osr : Models.OrderStatusReport = { orderId: orderId, time: ts.time };

        if (ts.data.result === "true") {
            osr.exchangeId = ts.data.order_id.toString();
            osr.orderStatus = Models.OrderStatus.Working;
        }
        else {
            osr.orderStatus = Models.OrderStatus.Rejected;
        }

        this.OrderUpdate.trigger(osr);
    };

    cancelOrder = (cancel : Models.BrokeredCancel) : Models.OrderGatewayActionReport => {
        var c : Cancel = {order_id: cancel.exchangeId, symbol: this._symbolProvider.symbol };
        this._socket.send<OrderAck>("ok_spotusd_cancel_order", this._signer.signMessage(c));
        return new Models.OrderGatewayActionReport(Utils.date());
    };

    private onCancel = (ts: Models.Timestamped<OrderAck>) => {
        var osr : Models.OrderStatusReport = { exchangeId: ts.data.order_id.toString(), time: ts.time };

        if (ts.data.result === "true") {
            osr.orderStatus = Models.OrderStatus.Cancelled;
        }
        else {
            osr.orderStatus = Models.OrderStatus.Rejected;
            osr.cancelRejected = true;
        }

        this.OrderUpdate.trigger(osr);
    };

    replaceOrder = (replace : Models.BrokeredReplace) : Models.OrderGatewayActionReport => {
        this.cancelOrder(new Models.BrokeredCancel(replace.origOrderId, replace.orderId, replace.side, replace.exchangeId));
        return this.sendOrder(replace);
    };

    private static getStatus(status: number) : Models.OrderStatus {
        // status: -1: cancelled, 0: pending, 1: partially filled, 2: fully filled, 4: cancel request in process
        switch (status) {
            case -1: return Models.OrderStatus.Cancelled;
            case 0: return Models.OrderStatus.Working;
            case 1: return Models.OrderStatus.Working;
            case 2: return Models.OrderStatus.Complete;
            case 4: return Models.OrderStatus.Working;
            default: return Models.OrderStatus.Other;
        }
    }

    private onTrade = (tsMsg : Models.Timestamped<BitMEXTradeRecord>) => {
        var t = tsMsg.time;
        var msg : BitMEXTradeRecord = tsMsg.data;

        var avgPx = parseFloat(msg.averagePrice);
        var lastQty = parseFloat(msg.sigTradeAmount);
        var lastPx = parseFloat(msg.sigTradePrice);

        var status : Models.OrderStatusReport = {
            exchangeId: msg.orderId.toString(),
            orderStatus: BitMEXOrderEntryGateway.getStatus(msg.status),
            time: t,
            lastQuantity: lastQty > 0 ? lastQty : undefined,
            lastPrice: lastPx > 0 ? lastPx : undefined,
            averagePrice: avgPx > 0 ? avgPx : undefined,
            pendingCancel: msg.status === 4,
            partiallyFilled: msg.status === 1
        };

        this.OrderUpdate.trigger(status);
    };

    private _log : Utils.Logger = Utils.log("tribeca:gateway:BitMEXOE");
    constructor(
            private _socket : BitMEXWebsocket,
            private _signer: BitMEXMessageSigner,
            private _symbolProvider: BitMEXSymbolProvider) {
        _socket.setHandler("ok_usd_realtrades", this.onTrade);
        _socket.setHandler("ok_spotusd_trade", this.onOrderAck);
        _socket.setHandler("ok_spotusd_cancel_order", this.onCancel);

        _socket.ConnectChanged.on(cs => {
            this.ConnectChanged.trigger(cs);

            if (cs === Models.ConnectivityStatus.Connected) {
                _socket.send("ok_usd_realtrades", _signer.signMessage({}));
            }
        });
    }
}

class BitMEXMessageSigner {
    private _secretKey : string;
    private _api_key : string;

    public signMessage = (m : SignedMessage) : SignedMessage => {
        var els : string[] = [];

        if (!m.hasOwnProperty("api_key"))
            m.api_key = this._api_key;

        var keys = [];
        for (var key in m) {
            if (m.hasOwnProperty(key))
                keys.push(key);
        }
        keys.sort();

        for (var i = 0; i < keys.length; i++) {
            var key = keys[i];
            if (m.hasOwnProperty(key))
                els.push(key + "=" + m[key]);
        }

        var sig = els.join("&") + "&secret_key=" + this._secretKey;
        m.sign = crypto.createHash('md5').update(sig).digest("hex").toString().toUpperCase();
        return m;
    };

    constructor(config : Config.IConfigProvider) {
        this._api_key = config.GetString("BitMEXApiKey");
        this._secretKey = config.GetString("BitMEXSecretKey");
    }
}

class BitMEXHttp {
    post = <T>(actionUrl: string, msg : SignedMessage) : Q.Promise<Models.Timestamped<T>> => {
        var d = Q.defer<Models.Timestamped<T>>();

        request({
            url: url.resolve(this._baseUrl, actionUrl),
            body: querystring.stringify(this._signer.signMessage(msg)),
            headers: {"Content-Type": "application/x-www-form-urlencoded"},
            method: "POST"
        }, (err, resp, body) => {
            if (err) d.reject(err);
            else {
                try {
                    var t = Utils.date();
                    var data = JSON.parse(body);
                    d.resolve(new Models.Timestamped(data, t));
                }
                catch (e) {
                    this._log("url: %s, err: %o, body: %o", actionUrl, err, body);
                    d.reject(e);
                }
            }
        });

        return d.promise;
    };

    private _log : Utils.Logger = Utils.log("tribeca:gateway:BitMEXHTTP");
    private _baseUrl : string;
    constructor(config : Config.IConfigProvider, private _signer: BitMEXMessageSigner) {
        this._baseUrl = config.GetString("BitMEXHttpUrl")
    }
}

class BitMEXPositionGateway implements Interfaces.IPositionGateway {
    PositionUpdate = new Utils.Evt<Models.CurrencyPosition>();

    private static convertCurrency(name : string) : Models.Currency {
        switch (name.toLowerCase()) {
            case "usd": return Models.Currency.USD;
            case "ltc": return Models.Currency.LTC;
            case "btc": return Models.Currency.BTC;
            default: throw new Error("Unsupported currency " + name);
        }
    }

    private trigger = () => {
        this._http.post("userinfo.do", {}).then(msg => {
            var free = (<any>msg.data).info.funds.free;
            var freezed = (<any>msg.data).info.funds.freezed;

            for (var currencyName in free) {
                if (!free.hasOwnProperty(currencyName)) continue;
                var amount = parseFloat(free[currencyName]);
                var held = parseFloat(freezed[currencyName]);

                var pos = new Models.CurrencyPosition(amount, held, BitMEXPositionGateway.convertCurrency(currencyName));
                this.PositionUpdate.trigger(pos);
            }
        }).done();
    };

    private _log : Utils.Logger = Utils.log("tribeca:gateway:BitMEXPG");
    constructor(private _http : BitMEXHttp) {
        setInterval(this.trigger, 15000);
        setTimeout(this.trigger, 10);
    }
}

class BitMEXBaseGateway implements Interfaces.IExchangeDetailsGateway {
    public get hasSelfTradePrevention() {
        return false;
    }

    name() : string {
        return "BitMEX";
    }

    makeFee() : number {
        return 0.001;
    }

    takeFee() : number {
        return 0.002;
    }

    exchange() : Models.Exchange {
        return Models.Exchange.BitMEX;
    }

    private static AllPairs = [
        new Models.CurrencyPair(Models.Currency.BTC, Models.Currency.USD),
        //new Models.CurrencyPair(Models.Currency.LTC, Models.Currency.USD),
    ];
    public get supportedCurrencyPairs() {
        return BitMEXBaseGateway.AllPairs;
    }
}

function GetCurrencyEnum(c: string) : Models.Currency {
    switch (name.toLowerCase()) {
        case "usd": return Models.Currency.USD;
        case "ltc": return Models.Currency.LTC;
        case "btc": return Models.Currency.BTC;
        default: throw new Error("Unsupported currency " + name);
    }
}

function GetCurrencySymbol(c: Models.Currency) : string {
    switch (c) {
        case Models.Currency.USD: return "usd";
        case Models.Currency.LTC: return "ltc";
        case Models.Currency.BTC: return "btc";
        default: throw new Error("Unsupported currency " + Models.Currency[c]);
    }
}

class BitMEXSymbolProvider {
    public symbol : string;
    public symbolWithoutUnderscore: string;

    constructor(pair: Models.CurrencyPair) {
        this.symbol = GetCurrencySymbol(pair.base) + "_" + GetCurrencySymbol(pair.quote);
        this.symbolWithoutUnderscore = GetCurrencySymbol(pair.base) + GetCurrencySymbol(pair.quote);
    }
}

export class BitMEX extends Interfaces.CombinedGateway {
    constructor(config : Config.IConfigProvider, pair: Models.CurrencyPair) {
        var symbol = new BitMEXSymbolProvider(pair);
        var signer = new BitMEXMessageSigner(config);
        var http = new BitMEXHttp(config, signer);
        var socket = new BitMEXWebsocket(config);

        var orderGateway = config.GetString("BitMEXOrderDestination") == "BitMEX"
            ? <Interfaces.IOrderEntryGateway>new BitMEXOrderEntryGateway(socket, signer, symbol)
            : new NullGateway.NullOrderGateway();

        super(
            new BitMEXMarketDataGateway(socket, symbol),
            orderGateway,
            new BitMEXPositionGateway(http),
            new BitMEXBaseGateway());
        }
}
