from pydantic import BaseModel, Field
from typing import List, Optional

class CompanyGold(BaseModel):
    nif: str = Field(..., description="Tax Identification Number")
    name: Optional[str] = Field(None, description="Company Name")
    
    # Aggregated stats from Contracts
    total_contracts: int = Field(0, description="Total number of contracts found")
    total_amount: float = Field(0.0, description="Sum of contract values")
    
    # Enriched data from Marketing/Other
    sector: Optional[str] = None
    email: Optional[str] = None
    address: Optional[str] = None
    
    source_ids: List[str] = Field(default_factory=list, description="List of source IDs merged into this record")
