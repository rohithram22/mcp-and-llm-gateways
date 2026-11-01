"""Input rules for each tool.

Each Pydantic model is used twice:
  1. to VALIDATE the arguments of a tools/call, and
  2. to GENERATE the inputSchema the LLM sees in tools/list.
"""

from pydantic import BaseModel, ConfigDict, Field, field_validator

CUSTOMER_ID_PATTERN = r"^CUST-\d{5}$"

STRICT = ConfigDict(strict=True, extra="forbid")


class GetCustomerRecordInput(BaseModel):
    model_config = STRICT

    customer_id: str = Field(
        pattern=CUSTOMER_ID_PATTERN,
        description="Customer ID in the form CUST-XXXXX (5 digits), e.g. CUST-00001",
    )


class TriggerRefundInput(BaseModel):
    model_config = STRICT

    customer_id: str = Field(
        pattern=CUSTOMER_ID_PATTERN,
        description="Customer ID in the form CUST-XXXXX (5 digits), e.g. CUST-00001",
    )
    amount: float = Field(
        gt=0,
        le=1_000_000,
        allow_inf_nan=False,
        description="Refund amount: positive, at most 2 decimal places",
    )
    reason: str = Field(
        min_length=10,
        max_length=500,
        description="Why the refund is being issued (at least 10 characters)",
    )

    @field_validator("amount")
    @classmethod
    def at_most_two_decimals(cls, value: float) -> float:
        if round(value, 2) != value:
            raise ValueError("amount must have at most 2 decimal places")
        return value

    @field_validator("reason")
    @classmethod
    def reason_not_just_spaces(cls, value: str) -> str:
        value = value.strip()
        if len(value) < 10:
            raise ValueError("reason must have at least 10 characters, not counting spaces")
        return value


# Lookup table: tool name -> (input model, description for the LLM)
TOOLS = {
    "get_customer_record": (
        GetCustomerRecordInput,
        "Look up a customer's record by customer ID.",
    ),
    "trigger_refund": (
        TriggerRefundInput,
        "Issue a refund to a customer.",
    ),
}