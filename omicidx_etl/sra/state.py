from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Optional
from prefect.variables import Variable
from prefect import get_run_logger
import json
import hashlib

class CompactSRAState(BaseModel):
    """Compact state tracking for SRA ETL process - fits in 5KB Variable limit"""
    current_month: str = Field(description="YYYY-MM format")
    last_full_dump: Optional[datetime] = None
    # Use List instead of Set to avoid JSON serialization issues
    processed_url_hashes: List[str] = Field(default_factory=list)
    completed_entities: List[str] = Field(default_factory=list)
    total_partitions_processed: int = 0
    
    def add_processed_url(self, url: str):
        """Add a URL to processed set using hash to save space"""
        url_hash = hashlib.md5(url.encode()).hexdigest()[:16]  # Use first 16 chars
        if url_hash not in self.processed_url_hashes:
            self.processed_url_hashes.append(url_hash)
            self.total_partitions_processed += 1
        
    def is_url_processed(self, url: str) -> bool:
        """Check if a URL has been processed"""
        url_hash = hashlib.md5(url.encode()).hexdigest()[:16]
        return url_hash in self.processed_url_hashes
    
    def get_unprocessed_urls(self, all_urls: List[str]) -> List[str]:
        """Get list of URLs that haven't been processed"""
        return [url for url in all_urls if not self.is_url_processed(url)]
    
    def mark_entity_complete(self, entity: str):
        """Mark an entity as completed"""
        if entity not in self.completed_entities:
            self.completed_entities.append(entity)
    
    def is_entity_complete(self, entity: str) -> bool:
        """Check if an entity has been completed"""
        return entity in self.completed_entities

def load_sra_state(var_name: str = "sra-etl-state") -> CompactSRAState:
    """Load compact SRA state from Prefect Variable"""
    try:
        logger = get_run_logger()
        state_json = Variable.get(var_name)
        if state_json:
            state_dict = json.loads(state_json)
            return CompactSRAState(**state_dict)
    except Exception as e:
        logger = get_run_logger()
        logger.info(f"No existing variable state found or error loading: {e}")
    
    # Return new state with current month
    current_month = datetime.now().strftime("%Y-%m")
    return CompactSRAState(current_month=current_month)

def save_sra_state(state: CompactSRAState, var_name: str = "sra-etl-state"):
    """Save compact SRA state to Prefect Variable"""
    logger = get_run_logger()
    try:
        state_json = json.dumps(state.model_dump(), default=str)
        
        # Check size constraint
        if len(state_json) > 4500:  # Leave some buffer
            logger.warning(f"State size ({len(state_json)} chars) approaching 5KB limit")
            # Could implement pruning here if needed
            
        Variable.set(var_name, state_json, overwrite=True)
        logger.info(f"State saved to variable: {var_name} ({len(state_json)} chars)")
    except Exception as e:
        logger.error(f"Error saving state to variable: {e}")
        raise

# Simple aliases for backward compatibility
def sync_load_sra_state(**kwargs) -> CompactSRAState:
    """Load SRA state"""
    return load_sra_state(**kwargs)

def sync_save_sra_state(state: CompactSRAState, **kwargs):
    """Save SRA state"""
    save_sra_state(state, **kwargs)
