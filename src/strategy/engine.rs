use crate::types::{common::Venue, kline::Kline, orderbook::Orderbook};
trait Engine{
    fn start(); // the main facade that polls the spsc queues for md, then does the recon process
    fn disconnect();
    // recon is basically requesting order snapshots, account balance, positions snapshots from the TRADE SERVER
    fn start_recon();
    fn recon_done();
    fn recon_success();
    fn recon_failure();
    // events -> to invoke handlers in strategy
    // these are meant to update the internal state of the engine first, before calling handlers
    fn on_order_update();
    fn on_fill();
    fn on_position_update();
    // these are the main crux of strategy
    fn on_orderbook(orderbook: Orderbook);
    fn on_kline(kline: Kline);
    // these are meant to be for looking at the internal state
    fn get_open_orders(); // includes PENDING NEWS
    fn get_open_positions();
    fn get_account_balance();
    // venue specific
    fn get_venue_open_orders(venue: Venue);
    fn get_venue_open_positions(venue: Venue);
    fn get_venue_account_balance(venue: Venue);
}