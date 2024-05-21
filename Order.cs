namespace ServiceBusQueue
{
    public class Order {
        
        public Guid OrderId {get; set;}
        public int Quantity {get; set;}
        public float UnitPrice {get; set;}
    }

    public class InvalidOrder {
        public string OrdId {get; set;}
        public long Qty {get; set;}
        public double Price {get; set;}
        public string Desc {get; set;}
    }
}