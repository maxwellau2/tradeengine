
pub trait StateSubscription{
    fn subscribe_order_updates();
    fn subscribe_position_updates();
    fn subscribe_balance_updates();
}