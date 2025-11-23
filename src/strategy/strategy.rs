use crate::{state_management::order_manager::Order, types::{kline::Kline, orderbook::{Level, Orderbook}}};
trait Strategy{
    fn on_orderbook(orderbook: Orderbook);
    fn on_kline(kline: Kline);
    fn on_start();
    fn on_disconnect();
    fn on_recon();
    fn on_recon_done();
    fn on_recon_success();
    fn on_recon_fail();
    fn on_order_update();
    fn on_fill();
    fn on_position_update();
}