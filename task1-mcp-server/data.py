"""A fake in-memory customer database, so the server runs without a real backend."""

import uuid
from datetime import datetime, timezone

CUSTOMERS = {
    "CUST-00001": {"name": "Ada Lovelace", "email": "ada@example.com", "tier": "gold", "refundable_balance": 250.00},
    "CUST-00002": {"name": "Alan Turing", "email": "alan@example.com", "tier": "silver", "refundable_balance": 80.50},
    "CUST-00003": {"name": "Grace Hopper", "email": "grace@example.com", "tier": "bronze", "refundable_balance": 0.00},
}


class CustomerNotFound(Exception):
    """The ID was well-formed, but no such customer exists."""


class RefundRejected(Exception):
    """The refund request was valid, but the business rules say no."""


def get_customer(customer_id: str) -> dict:
    record = CUSTOMERS.get(customer_id)
    if record is None:
        raise CustomerNotFound(f"No customer found with id {customer_id}")
    return {"customer_id": customer_id, **record}


def create_refund(customer_id: str, amount: float, reason: str) -> dict:
    customer = get_customer(customer_id)
    balance = customer["refundable_balance"]
    if amount > balance:
        raise RefundRejected(f"Refund of {amount:.2f} exceeds refundable balance of {balance:.2f}")

    new_balance = round(balance - amount, 2)
    CUSTOMERS[customer_id]["refundable_balance"] = new_balance
    return {
        "refund_id": f"RF-{uuid.uuid4().hex[:8].upper()}",
        "customer_id": customer_id,
        "amount": amount,
        "reason": reason,
        "status": "processed",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "remaining_refundable_balance": new_balance,
    }