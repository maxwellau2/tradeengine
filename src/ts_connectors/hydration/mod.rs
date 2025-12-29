// hydration is meant to be used on startup and on retry/connection failure

pub trait HydrationConnector {
    fn get_balance();
    fn get_open_orders();
    fn get_positions();
}
