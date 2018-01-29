package pl.piomin.services.vertx.order.model;

public class Order {

	private Long id;
	private OrderType type;
	private OrderStatus status;
	private int price;
	private Long relatedOrderId;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public OrderType getType() {
		return type;
	}

	public void setType(OrderType type) {
		this.type = type;
	}

	public OrderStatus getStatus() {
		return status;
	}

	public void setStatus(OrderStatus status) {
		this.status = status;
	}

	public int getPrice() {
		return price;
	}

	public void setPrice(int price) {
		this.price = price;
	}

	public Long getRelatedOrderId() {
		return relatedOrderId;
	}

	public void setRelatedOrderId(Long relatedOrderId) {
		this.relatedOrderId = relatedOrderId;
	}

}
